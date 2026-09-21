"""Offline summaries from retained attempts, including tails and missing returns."""

from array import array
from collections import Counter
import json
import math
import os
import statistics

from adapter import RequestFailure, require_success
from runtime import EnvironmentLimit, GIB, proc_sample


def summarize_runs(results, planned_runs, role, scenario, repeats):
    """Keep completed observations, service outcomes and correctness verdicts distinct."""
    groups = []
    if scenario in ("P1", "P2"):
        for clients, variant in sorted({(item["clients"], item["variant"]) for item in results}):
            group = [item for item in results if item["clients"] == clients and item["variant"] == variant]
            complete = len(group) == repeats and all(item.get("validated_comparison") for item in group)
            values = [item["logical_summary"]["success_ops_per_second"] for item in group
                      if item.get("validated_comparison")]
            groups.append({"clients": clients, "variant": variant, "all_planned_runs_validated": complete,
                           "success_ops_per_second": values,
                           "median": statistics.median(values) if values and complete else None,
                           "range": [min(values), max(values)] if values and complete else None})
    summary = {"format_version": 2, "role": role, "planned_runs": planned_runs,
               "finished_runs": len(results), "groups": groups,
               "all_planned_observations_validated": len(results) == planned_runs and
               all(item.get("validated_comparison", item["status"] == "passed") for item in results)}
    if scenario.startswith("C"):
        summary["all_planned_runs_passed"] = len(results) == planned_runs and all(
            item["status"] == "passed" for item in results)
    return summary


def records(path):
    with path.open(encoding="utf-8") as source:
        for line in source:
            yield json.loads(line)


def joined_history(root):
    """Merge monotonically allocated input IDs with out-of-order bounded completions."""
    inputs = iter(records(root / "inputs.jsonl"))
    pending = {}
    for event in records(root / "history.jsonl"):
        correlation = event["correlation"]
        while correlation not in pending:
            try:
                item = next(inputs)
            except StopIteration as error:
                raise ValueError("history references missing input") from error
            pending[item["correlation"]] = item
            if item["correlation"] > correlation:
                raise ValueError("history repeats an already finished input")
        item = pending[correlation]
        if event["event"] == "call":
            item["observed_call_ns"] = event["call_ns"]
        yield item, event
        if event["event"] != "call":
            del pending[correlation]
    for item in inputs:
        pending[item["correlation"]] = item
    for item in pending.values():
        yield item, {"event": "missing_return" if "observed_call_ns" in item else "unobserved",
                     "call_ns": item.get("observed_call_ns")}


def percentiles(samples):
    if not samples:
        return {"count": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None}
    # Exact sorting needs boxed Python integers and scratch space. Refuse to
    # silently sample/drop long-tail observations to make an oversized report fit.
    if proc_sample(os.getpid())["rss_bytes"] + len(samples) * 48 > GIB // 2:
        raise EnvironmentLimit("exact percentile report exceeds the 512 MiB observer budget; raw history retained")
    samples = sorted(samples)
    return {"count": len(samples), **{
        name: samples[max(0, math.ceil(len(samples) * quantile) - 1)] / 1e6
        for name, quantile in (("p50_ms", 0.50), ("p95_ms", 0.95), ("p99_ms", 0.99))}}


def outcome_of(item, event):
    if event["event"] in ("missing_return", "unobserved"):
        return "UNKNOWN" if item["kind"] == "WRITE" else "INCOMPLETE"
    try:
        require_success(event, write=item["kind"] == "WRITE")
        return "SUCCESS"
    except RequestFailure as error:
        return error.outcome


class WindowMetrics:
    def __init__(self, measurement):
        self.start, self.end = measurement["start_ns"], measurement["end_ns"]
        self.calls = self.completed = self.tail = self.outside = 0
        self.successes, self.failures = array("Q"), array("Q")
        self.outcomes = Counter()

    def observe(self, event, outcome=None):
        if event["event"] == "call":
            self.calls += self.start <= event["call_ns"] < self.end
            return
        self.outcomes[outcome] += 1
        if event["event"] in ("skipped", "missing_return", "unobserved"):
            return
        if not self.start <= event["call_ns"] < self.end:
            self.outside += 1
            return
        latency = event["return_ns"] - event["call_ns"]
        if latency < 0:
            raise ValueError("return precedes invocation")
        if outcome == "SUCCESS":
            self.successes.append(latency)
            self.completed += event["return_ns"] < self.end
            self.tail += event["return_ns"] >= self.end
        else:
            self.failures.append(latency)

    def result(self):
        seconds = (self.end - self.start) / 1e9
        if seconds <= 0:
            raise ValueError("measurement must have positive duration")
        return {"window_seconds": seconds, "window_calls": self.calls, "window_successes": self.completed,
                "tail_successes": self.tail, "out_of_window_calls": self.outside,
                "success_ops_per_second": self.completed / seconds,
                "successful_call_latency": percentiles(self.successes),
                "failed_call_latency": percentiles(self.failures), "outcomes": dict(self.outcomes),
                "p99_exploratory": len(self.successes) < 10000,
                "latency_scope": "driver invocation to validated client return; window-started operations include drain tails"}


def summarize(root, measurement=None, phase="measure"):
    counts, phases = Counter(), Counter()
    window = WindowMetrics(measurement) if measurement else None
    for item, event in joined_history(root):
        if item["kind"] == "CLOCK":
            continue
        outcome = None if event["event"] == "call" else outcome_of(item, event)
        if outcome is not None:
            counts[outcome] += 1
            phases[f"{item['phase']}/{outcome}"] += 1
            if event["event"] in ("missing_return", "unobserved"):
                counts[event["event"]] += 1
        if window and item["phase"] == phase:
            window.observe(event, outcome)
    result = {"all_attempt_outcomes": dict(counts), "phase_outcomes": dict(phases)}
    if window:
        result.update(window.result())
        if window.outside:
            result["all_attempt_outcomes"]["out_of_window_call"] = window.outside
    return result


def summarize_windows(root, windows):
    metrics = {window["phase"]: WindowMetrics(window) for window in windows}
    # P3 can contain 200,000 operations. Read its trace once for all rounds.
    for item, event in joined_history(root):
        if item["phase"] in metrics:
            metrics[item["phase"]].observe(event, None if event["event"] == "call" else outcome_of(item, event))
    return {phase: metric.result() for phase, metric in metrics.items()}


def trend(values):
    """Finite-window criterion, not a proof of permanent leak freedom."""
    if len(values) != 5 or any(value <= 0 for value in values):
        return {"status": "insufficient_samples"}
    mean = sum(values) / 5
    slope = sum((index - 2) * value for index, value in enumerate(values)) / 10
    span = max(values) - min(values)
    return {"status": "stable_window" if abs(slope) <= mean * 0.01 and span <= mean * 0.05 else "observed_growth_or_variation",
            "mean_bytes": mean, "slope_bytes_per_round": slope, "range_bytes": span,
            "slope_fraction": slope / mean, "range_fraction": span / mean}


def summarize_space(root):
    all_rounds = list(records(root / "space_rounds.jsonl"))
    rounds = [row for row in all_rounds if row.get("complete", True)]
    peaks = {row["round"]: max(row["before"]["total_bytes"], row["after"]["total_bytes"]) for row in rounds}
    samples = Counter()
    for sample in records(root / "resources.jsonl"):
        for row in rounds:
            if row["python_start_ns"] <= sample["monotonic_ns"] <= row["python_end_ns"]:
                samples[row["round"]] += 1
                peaks[row["round"]] = max(peaks[row["round"]], sample["storage_usage"]["total_bytes"])
                break
    last = rounds[-5:]
    metrics = summarize_windows(root, rounds)
    covered = (len(rounds) == 10 and all(samples[row["round"]] for row in last) and
               rounds[-1]["observed_retention_advances"] - rounds[-6]["observed_retention_advances"] >= 3 and
               all(row["after"]["kind"] == "filesystem_allocated_blocks" for row in last))
    ends = trend([row["after"]["total_bytes"] for row in last])
    peak_trend = trend([peaks[row["round"]] for row in last])
    result = {"status": "not_covered", "partial_rounds": [row for row in all_rounds if not row.get("complete", True)],
              "round_end_trend": ends, "sampled_peak_trend": peak_trend,
              "scope": "allocated database file blocks; 1Hz observed peaks, not exact peaks or physical flash writes",
              "rounds": [{**row, "sampled_peak_bytes": peaks[row["round"]],
                          "resource_samples": samples[row["round"]],
                          "metrics": metrics[row["phase"]]} for row in rounds]}
    if covered and ends["status"] != "insufficient_samples" and peak_trend["status"] != "insufficient_samples":
        result["status"] = "stable_window" if ends["status"] == peak_trend["status"] == "stable_window" else "observed_growth_or_variation"
    return result


def summarize_arrivals(root):
    if (root / "logical_history.jsonl").exists():
        return summarize_logical_arrivals(root)
    timeline = json.loads((root / "recovery_timeline.json").read_text())
    dispatch = json.loads((root / "arrival_dispatch.json").read_text())
    boundaries = (("healthy", "start_ns", "stop_follower_ns"),
                  ("follower_offline", "stop_follower_ns", "restart_issued_ns"),
                  ("recovering_k0", "restart_issued_ns", "recovered_k0_ns"),
                  ("after_k0", "recovered_k0_ns", "end_ns"))
    windows = {name: {"start_ns": timeline[start], "end_ns": timeline[end], "planned": 0,
                      "admitted": 0, "completed_in_window": 0, "outcomes": Counter(),
                      "service_success": array("Q"), "service_failure": array("Q"),
                      "schedule_success": array("Q"), "schedule_failure": array("Q"), "queue": array("Q")}
               for name, start, end in boundaries if start in timeline and end in timeline}

    def at(stamp):
        return next((value for value in windows.values() if value["start_ns"] <= stamp < value["end_ns"]), None)

    for plan in records(root / "arrivals.jsonl"):
        window = at(plan["planned_ns"])
        if window is not None:
            window["planned"] += 1
            window["admitted"] += plan["admitted"]
            if not plan["admitted"]:
                window["outcomes"]["NOT_ISSUED_QUEUE_FULL"] += 1
    for item, event in joined_history(root):
        if item["phase"] != "foreground" or event["event"] == "call":
            continue
        window = at(item["planned_ns"])
        if window is None:
            raise ValueError("foreground plan outside retained timeline")
        if event["event"] in ("missing_return", "unobserved"):
            window["outcomes"]["UNKNOWN" if item["kind"] == "WRITE" else "INCOMPLETE"] += 1
            continue
        try:
            require_success(event, write=item["kind"] == "WRITE")
            outcome = "SUCCESS"
        except RequestFailure as error:
            outcome = error.outcome
        window["outcomes"][outcome] += 1
        if event["event"] == "skipped":
            continue
        queue_delay = event["call_ns"] - item["planned_ns"]
        duration = event["return_ns"] - event["call_ns"]
        if min(queue_delay, duration) < 0:
            raise ValueError("arrival timing precedes the plan or invocation")
        window["queue"].append(queue_delay)
        suffix = "success" if outcome == "SUCCESS" else "failure"
        window[f"service_{suffix}"].append(duration)
        window[f"schedule_{suffix}"].append(event["return_ns"] - item["planned_ns"])
        completion_window = at(event["return_ns"])
        if outcome == "SUCCESS" and completion_window is not None:
            completion_window["completed_in_window"] += 1
    result = {}
    for name, window in windows.items():
        seconds = (window["end_ns"] - window["start_ns"]) / 1e9
        latencies = {key: percentiles(window.pop(key)) for key in
                     ("service_success", "service_failure", "schedule_success", "schedule_failure", "queue")}
        result[name] = {**window, "outcomes": dict(window["outcomes"]), "latencies": latencies,
                        "window_seconds": seconds,
                        "success_ops_per_second": window["completed_in_window"] / seconds if seconds > 0 else None,
                        "p99_exploratory": latencies["service_success"]["count"] < 10000}
    return {"windows": result, "dispatch": dispatch,
            "cohort_scope": "latencies/outcomes grouped by planned time, including drain; throughput by completion time",
            "k0_scope": "fixed pre-restart boundary; does not claim current latest state until final verification"}


def summarize_resources(root, measurement):
    first = last = None
    peak_nodes = peak_clients = 0
    lifetimes = {"nodes": {}, "clients": {}}
    for sample in records(root / "resources.jsonl"):
        if measurement and not (measurement["python_start_ns"] <= sample["monotonic_ns"] <=
                                measurement["python_end_ns"]):
            continue
        first = first or sample
        last = sample
        peak_nodes = max(peak_nodes, sum(item["rss_bytes"] for item in sample["nodes"]))
        peak_clients = max(peak_clients, sum(item["rss_bytes"] for item in sample["clients"]))
        for group in lifetimes:
            for item in sample[group]:
                key = (item["pid"], item["start_ticks"])
                previous = lifetimes[group].get(key)
                lifetimes[group][key] = (previous[0] if previous else item, item)
    if first is None or last is first:
        return {"status": "insufficient_samples"}
    result = {"status": "observed", "scope": "sampled process lifetimes; gaps and before/after-sample IO excluded",
              "observation_seconds": (last["monotonic_ns"] - first["monotonic_ns"]) / 1e9,
              "peak_node_rss_bytes": peak_nodes, "peak_client_rss_bytes": peak_clients}
    for group, processes in lifetimes.items():
        cpu = 0
        io = {"read_bytes": 0, "write_bytes": 0}
        for before, after in processes.values():
            cpu += after["cpu_ticks"] - before["cpu_ticks"]
            for name in io:
                if name not in before["io"] or name not in after["io"]:
                    io[name] = None
                elif io[name] is not None:
                    io[name] += after["io"][name] - before["io"][name]
        result[group] = {"cpu_seconds": cpu / os.sysconf("SC_CLK_TCK"), "io": io,
                         "sampled_process_lifetimes": len(processes)}
    return result


def summarize_logical(root, measurement):
    """One sample per business observation; censored times are never exact samples."""
    phase_counts = Counter()
    outcomes = Counter()
    success, failure, censored = array("Q"), array("Q"), array("Q")
    completed = tail = retries = deadline_misses = missing = 0
    pending = {}
    start = measurement["python_start_ns"] if measurement else 0
    end = measurement["python_end_ns"] if measurement else 0
    phase = measurement.get("phase", "measure") if measurement else None
    for item in records(root / "logical_history.jsonl"):
        if item["event"] == "start":
            pending[item["logical_id"]] = item
            continue
        if pending.pop(item["logical_id"], None) is None:
            raise ValueError("logical result has no unique start")
        phase_counts[f"{item['phase']}/{item['outcome']}"] += 1
        if item["phase"] != phase or not start <= item["started_python_ns"] < end:
            continue
        outcome = item["outcome"]
        outcomes[outcome] += 1
        retries += max(0, item["attempts"] - 1)
        deadline_misses += item["observed_until_python_ns"] >= item["deadline_python_ns"]
        if outcome == "SUCCESS":
            success.append(item["elapsed_ns"])
            completed += item["observed_until_python_ns"] < end
            tail += item["observed_until_python_ns"] >= end
        elif outcome == "UNCONFIRMED":
            censored.append(item["elapsed_ns"])
        elif outcome == "FAILED":
            failure.append(item["elapsed_ns"])
    for item in pending.values():
        phase_counts[f"{item['phase']}/INCOMPLETE"] += 1
        if item["phase"] == phase and start <= item["started_python_ns"] < end:
            outcomes["INCOMPLETE"] += 1
            missing += 1
    total = sum(outcomes.values())
    seconds = (end - start) / 1e9 if measurement else None
    return {"phase_outcomes": dict(phase_counts), "outcomes": dict(outcomes),
            "window_seconds": seconds, "window_successes": completed, "tail_successes": tail,
            "success_ops_per_second": completed / seconds if seconds and seconds > 0 else None,
            "logical_operations": total, "retry_attempts": retries, "deadline_misses": deadline_misses,
            "success_fraction": outcomes["SUCCESS"] / total if total else None,
            "unconfirmed_fraction": outcomes["UNCONFIRMED"] / total if total else None,
            "successful_operation_latency": percentiles(success), "failed_operation_latency": percentiles(failure),
            "censored": {"count": len(censored), "min_observed_ms": min(censored) / 1e6 if censored else None,
                         "max_observed_ms": max(censored) / 1e6 if censored else None,
                         "display": "observed time +; success confirmation absent at cutoff"},
            "full_population_p99_ms": percentiles(success)["p99_ms"] if total and outcomes["SUCCESS"] == total else None,
            "p99_exploratory": len(success) < 10000,
            "observation_complete": missing == 0,
            "latency_scope": "logical start to confirmation; includes attempts, rediscovery/backoff; P4 includes queueing",
            "unknown_scope": "no successful confirmation observed; not evidence that the write did not commit"}


def summarize_logical_arrivals(root):
    timeline = json.loads((root / "recovery_timeline.json").read_text())
    dispatch = json.loads((root / "arrival_dispatch.json").read_text())
    stages = [("healthy", "start_ns"), ("follower_offline", "stop_follower_ns"),
              ("recovering_k0", "restart_issued_ns"), ("after_k0", "recovered_k0_ns")]
    available = [(name, timeline[key]) for name, key in stages if key in timeline]
    windows = {}
    for i, (name, begin) in enumerate(available):
        end = available[i + 1][1] if i + 1 < len(available) else timeline["end_ns"]
        windows[name] = {"start_ns": begin, "end_ns": end, "planned": 0, "admitted": 0,
                         "completed_in_window": 0, "outcomes": Counter(),
                         "schedule_success": array("Q"), "queue": array("Q"), "censored": 0}

    def at(stamp):
        return next((w for w in windows.values() if w["start_ns"] <= stamp < w["end_ns"]), None)

    unfinished = set()
    for plan in records(root / "arrivals.jsonl"):
        window = at(plan["planned_ns"])
        if window is None:
            raise ValueError("arrival generated outside declared observation window")
        window["planned"] += 1
        window["admitted"] += plan["admitted"]
        if plan["admitted"]:
            unfinished.add(plan["plan_id"])
        else:
            window["outcomes"]["NOT_ISSUED_" + plan.get("reason", "queue_full").upper()] += 1
    for item in records(root / "logical_history.jsonl"):
        if item["event"] != "end" or item["phase"] != "foreground":
            continue
        if item["plan_id"] not in unfinished:
            raise ValueError("logical operation repeats/misses an admitted arrival")
        unfinished.remove(item["plan_id"])
        window = at(item["planned_ns"])
        window["outcomes"][item["outcome"]] += 1
        window["queue"].append(max(0, item["dispatch_python_ns"] - item["started_python_ns"]))
        if item["outcome"] == "SUCCESS":
            window["schedule_success"].append(item["elapsed_ns"])
            completion = at(item["planned_ns"] + item["elapsed_ns"])
            if completion is not None:
                completion["completed_in_window"] += 1
        window["censored"] += item["outcome"] == "UNCONFIRMED"
    dispositions = {item["plan_id"]: item["reason"] for item in dispatch.get("unissued_plans", [])}
    for plan in records(root / "arrivals.jsonl"):
        if plan["plan_id"] in unfinished:
            at(plan["planned_ns"])["outcomes"]["NOT_ISSUED_" + dispositions.get(plan["plan_id"], "incomplete").upper()] += 1
    for window in windows.values():
        seconds = (window["end_ns"] - window["start_ns"]) / 1e9
        window.update(window_seconds=seconds, success_ops_per_second=window["completed_in_window"] / seconds if seconds > 0 else None,
                      outcomes=dict(window["outcomes"]), latencies={"schedule_success": percentiles(window.pop("schedule_success")),
                                                                  "queue": percentiles(window.pop("queue"))})
    return {"windows": windows, "dispatch": dispatch,
            "scope": "logical operations by planned arrival; retry traffic is not successful goodput"}
