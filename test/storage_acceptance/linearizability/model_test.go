package main

import (
	"github.com/anishathalye/porcupine"
	"testing"
	"time"
)

func TestBusinessHistories(t *testing.T) {
	initial := []string{"7", "3", "2", "10", "0", "order=ready"}
	write := func(id int, call, ret int64, outcome string) Attempt {
		result := ""
		if outcome == "SUCCESS" {
			result = "same-committed-result"
		}
		return Attempt{ID: id, Kind: "Bump", Client: 101, Request: 1, Intent: "original SQL bytes",
			LastOp: 1001, Call: call, Return: ret, Outcome: outcome, Result: result}
	}
	read := func(id int, call, ret int64, version, last string) Attempt {
		return Attempt{ID: id, Kind: "ReadPoint", Call: call, Return: ret, Outcome: "SUCCESS",
			Row: []string{"7", "3", "2", version, last, "order=ready"}}
	}
	cases := []struct {
		name     string
		attempts []Attempt
		want     porcupine.CheckResult
	}{
		{"timeout may execute after timeout", []Attempt{write(1, 1, 2, "UNKNOWN"), read(2, 3, 4, "10", "0"), read(3, 5, 6, "11", "1001")}, porcupine.Ok},
		{"retry does not increment twice", []Attempt{write(1, 1, 2, "UNKNOWN"), write(2, 3, 4, "SUCCESS"), read(3, 5, 6, "11", "1001")}, porcupine.Ok},
		{"duplicate execution rejected", []Attempt{write(1, 1, 2, "UNKNOWN"), write(2, 3, 4, "SUCCESS"), read(3, 5, 6, "12", "1001")}, porcupine.Illegal},
		{"old read after completed write rejected", []Attempt{write(1, 1, 2, "SUCCESS"), read(2, 3, 4, "10", "0")}, porcupine.Illegal},
		{"overlap permits old read", []Attempt{write(1, 1, 4, "SUCCESS"), read(2, 2, 3, "10", "0")}, porcupine.Ok},
		{"unknown cannot undo observed update", []Attempt{write(1, 1, 2, "UNKNOWN"), read(2, 3, 4, "11", "1001"), read(3, 5, 6, "10", "0")}, porcupine.Illegal},
		{"mixed row rejected", []Attempt{write(1, 1, 2, "SUCCESS"), read(2, 3, 4, "11", "0")}, porcupine.Illegal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := History{Format: 1, Initial: initial, End: 20, Attempts: c.attempts}
			ops, err := operations(h)
			if err != nil {
				t.Fatal(err)
			}
			got := porcupine.CheckOperationsTimeout(model(initial), ops, time.Second)
			if got != c.want {
				t.Fatalf("got %s; want %s", got, c.want)
			}
		})
	}
}
