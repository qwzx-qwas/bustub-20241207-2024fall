// Project-owned sequential model. Porcupine supplies search, not business answers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/anishathalye/porcupine"
)

type Attempt struct {
	ID      int      `json:"id"`
	Kind    string   `json:"kind"`
	Client  uint64   `json:"client_id"`
	Request uint64   `json:"request_id"`
	Intent  string   `json:"sql"`
	LastOp  int64    `json:"last_op"`
	Call    int64    `json:"call_ns"`
	Return  int64    `json:"return_ns"`
	Outcome string   `json:"outcome"`
	Row     []string `json:"row"`
	Result  string   `json:"result"`
}

type History struct {
	Format   int       `json:"format_version"`
	Initial  []string  `json:"initial"`
	End      int64     `json:"end_ns"`
	Attempts []Attempt `json:"attempts"`
}

type Applied struct{ Intent, Result string }
type State struct {
	Row  []string
	Seen map[string]Applied
}

func clone(s State) State {
	n := State{append([]string(nil), s.Row...), make(map[string]Applied, len(s.Seen))}
	for k, v := range s.Seen {
		n.Seen[k] = v
	}
	return n
}

func apply(s State, a Attempt) (State, bool) {
	key := fmt.Sprintf("%d/%d", a.Client, a.Request)
	n := clone(s)
	if old, exists := s.Seen[key]; exists {
		if old.Intent != a.Intent || (a.Result != "" && old.Result != "" && a.Result != old.Result) {
			return State{}, false
		}
		if a.Result != "" {
			n.Seen[key] = Applied{a.Intent, a.Result}
		}
		return n, true
	}
	version, err := strconv.ParseInt(s.Row[3], 10, 64)
	if err != nil {
		return State{}, false
	}
	n.Row[3] = strconv.FormatInt(version+1, 10)
	n.Row[4] = strconv.FormatInt(a.LastOp, 10)
	n.Seen[key] = Applied{a.Intent, a.Result}
	return n, true
}

func model(initial []string) porcupine.Model {
	nondeterministic := porcupine.NondeterministicModel{
		Init: func() []interface{} {
			return []interface{}{State{append([]string(nil), initial...), map[string]Applied{}}}
		},
		StepContext: func(ctx context.Context, state, input, _ interface{}) []interface{} {
			if ctx.Err() != nil {
				return nil
			}
			s, a := state.(State), input.(Attempt)
			if a.Outcome == "NO_EFFECT" {
				return []interface{}{s}
			}
			if a.Kind == "ReadPoint" {
				if a.Outcome == "UNKNOWN" || reflect.DeepEqual(a.Row, s.Row) {
					return []interface{}{s}
				}
				return nil
			}
			next, valid := apply(s, a)
			if a.Outcome == "UNKNOWN" {
				if valid {
					return []interface{}{s, next}
				}
				return []interface{}{s}
			}
			if valid {
				return []interface{}{next}
			}
			return nil
		},
		Equal: func(a, b interface{}) bool { return reflect.DeepEqual(a, b) },
		DescribeOperation: func(input, _ interface{}) string {
			a := input.(Attempt)
			return fmt.Sprintf("%s client=%d request=%d last_op=%d outcome=%s row=%v", a.Kind, a.Client, a.Request, a.LastOp, a.Outcome, a.Row)
		},
		DescribeState: func(state interface{}) string { return fmt.Sprintf("%v", state.(State).Row) },
	}
	return nondeterministic.ToModel()
}

func operations(h History) ([]porcupine.Operation, error) {
	if h.Format != 1 || len(h.Initial) != 6 || len(h.Attempts) == 0 || len(h.Attempts) > 128 {
		return nil, fmt.Errorf("invalid history version, initial row or attempt budget")
	}
	if _, err := strconv.ParseInt(h.Initial[3], 10, 64); err != nil {
		return nil, err
	}
	result := make([]porcupine.Operation, 0, len(h.Attempts))
	seen := map[int]bool{}
	for _, a := range h.Attempts {
		if seen[a.ID] || a.ID <= 0 || a.Call <= 0 || a.Return < a.Call || a.Return > h.End {
			return nil, fmt.Errorf("invalid history identity/time")
		}
		seen[a.ID] = true
		if a.Kind != "Bump" && a.Kind != "ReadPoint" {
			return nil, fmt.Errorf("unmodelled operation")
		}
		if a.Outcome != "SUCCESS" && a.Outcome != "UNKNOWN" && a.Outcome != "NO_EFFECT" {
			return nil, fmt.Errorf("unclassified outcome")
		}
		if a.Kind == "Bump" && (a.Client == 0 || a.Request == 0 || a.Intent == "" ||
			(a.Outcome == "SUCCESS" && a.Result == "")) {
			return nil, fmt.Errorf("missing write identity/body/result")
		}
		if a.Kind == "ReadPoint" && a.Outcome == "SUCCESS" && len(a.Row) != 6 {
			return nil, fmt.Errorf("read must contain exactly one complete row")
		}
		end := a.Return
		// Transport timeout is not cancellation. Preserve the raw return while
		// allowing an unknown attempt to take effect later, through observation end.
		if a.Outcome == "UNKNOWN" {
			end = h.End
		}
		result = append(result, porcupine.Operation{ClientId: len(result), Input: a,
			Call: a.Call, Return: end, Output: a.Outcome, Metadata: a.ID})
	}
	return result, nil
}

func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("usage: check-history INPUT_JSON OUTPUT_DIRECTORY")
	}
	input, err := os.Open(os.Args[1])
	if err != nil {
		return err
	}
	defer input.Close()
	var h History
	decoder := json.NewDecoder(io.LimitReader(input, 16<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&h); err != nil {
		return err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("history has trailing data")
	}
	ops, err := operations(h)
	if err != nil {
		return err
	}
	m := model(h.Initial)
	outcome, info := porcupine.CheckOperationsVerbose(m, ops, 30*time.Second)
	// Write the decision before optional visualization; external runner still
	// requires a clean checker exit and enforces process time/RSS budgets.
	output, err := os.Create(os.Args[2] + "/check.json")
	if err != nil {
		return err
	}
	err = json.NewEncoder(output).Encode(map[string]interface{}{
		"format_version": 1, "checker": "porcupine-v1.3.0", "result": outcome,
		"attempts": len(ops), "scope": "single-key Bump/ReadPoint plus request deduplication"})
	closeErr := output.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	html, err := os.Create(os.Args[2] + "/history.html")
	if err != nil {
		return err
	}
	defer html.Close()
	return porcupine.Visualize(m, info, html)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
