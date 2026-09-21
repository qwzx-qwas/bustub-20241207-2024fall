"""Preserve network attempts and check the project's small business history offline."""

import json
import os
import subprocess
import time

from adapter import RequestFailure, require_success
from report import joined_history
from runtime import EnvironmentLimit, GIB, proc_sample, write_json


def export_history(root):
    initial = json.loads((root / "history_initial.json").read_text())
    boundary = json.loads((root / "history_boundary.json").read_text())
    calls, attempts = {}, []
    for item, event in joined_history(root):
        if item["phase"] != "history":
            continue
        operation = item["operation"]
        if operation["name"] not in ("Bump", "ReadPoint") or operation["args"]["id"] != int(initial[0]):
            raise ValueError("history contains an operation outside its one-key model")
        correlation = item["correlation"]
        if event["event"] == "call":
            calls[correlation] = event["call_ns"]
            continue
        if event["event"] in ("unobserved", "skipped") or correlation not in calls:
            raise ValueError("history attempt has no observed invocation; incomplete history")
        outcome = "UNKNOWN"
        if event["event"] != "missing_return":
            try:
                require_success(event, write=item["kind"] == "WRITE")
                outcome = "SUCCESS"
            except RequestFailure as error:
                # A node can append this write, step down while waiting, then
                # reply NOT_LEADER. That does not cancel the replicated entry.
                if error.outcome == "NOT_LEADER" and item["kind"] == "WRITE":
                    outcome = "UNKNOWN"
                elif error.outcome in ("REJECTED", "NOT_LEADER"):
                    outcome = "NO_EFFECT"
                elif error.outcome != "UNKNOWN":
                    raise ValueError(f"uncheckable response: {error}") from error
        row = []
        if item["kind"] == "READ" and outcome == "SUCCESS":
            rows = event.get("rows", [])
            if len(rows) != 1 or len(rows[0]) != 6:
                raise AssertionError("successful history read lost or duplicated the shared row")
            row = rows[0]
        attempts.append({"id": correlation, "kind": operation["name"], "client_id": item["client_id"],
                         "request_id": item["request_id"], "sql": item["sql"],
                         "last_op": operation["args"].get("last_op", 0), "call_ns": calls[correlation],
                         "return_ns": event.get("return_ns", boundary["end_ns"]), "outcome": outcome,
                         "row": row, "result": event.get("payload_hex", "") if outcome == "SUCCESS" else ""})
    if not 1 <= len(attempts) <= 128:
        raise EnvironmentLimit("history must contain 1..128 attempts; no splitting or dropping")
    write_json(root / "linearizability.json", {"format_version": 1, "initial": initial,
                                               "end_ns": boundary["end_ns"], "attempts": attempts})
    successful = [entry for entry in attempts if entry["outcome"] == "SUCCESS"]
    overlap = any(a["call_ns"] < b["return_ns"] and b["call_ns"] < a["return_ns"]
                  for i, a in enumerate(attempts) for b in attempts[i + 1:])
    return {"attempts": len(attempts), "actual_network_overlap": overlap,
            "successful_writes": sum(a["kind"] == "Bump" for a in successful),
            "successful_reads": sum(a["kind"] == "ReadPoint" for a in successful),
            "post_fault_successes": sum(a["call_ns"] >= boundary.get("fault_end_ns", 2**63 - 1)
                                        for a in successful)}


def check_history(root, binary):
    coverage = export_history(root)
    environment = dict(os.environ, GOMEMLIMIT="768MiB", GOMAXPROCS="1")
    with (root / "checker.stderr").open("xb") as stderr:
        process = subprocess.Popen([str(binary), str(root / "linearizability.json"), str(root)],
                                   stdout=stderr, stderr=stderr, env=environment)
        deadline = time.monotonic() + 45  # search 30 seconds; bounded input/visualization allowance
        peak = 0
        try:
            while process.poll() is None:
                if time.monotonic() >= deadline:
                    raise EnvironmentLimit("offline checker exceeded 45 seconds including visualization")
                try:
                    peak = max(peak, proc_sample(process.pid)["rss_bytes"])
                except FileNotFoundError:
                    if process.poll() is None:
                        raise
                if peak > GIB:
                    raise EnvironmentLimit("offline checker exceeded 1 GiB observed RSS")
                time.sleep(0.05)
            if process.returncode != 0:
                raise RuntimeError("checker did not complete; see checker.stderr")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    decision = json.loads((root / "check.json").read_text())
    if decision.get("result") not in ("Ok", "Illegal", "Unknown"):
        raise ValueError("invalid checker decision")
    return {**decision, "coverage": coverage, "peak_rss_bytes": peak}
