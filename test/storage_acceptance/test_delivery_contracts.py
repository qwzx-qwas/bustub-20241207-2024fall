"""Protect measurement promises; scripted responses do not establish DB correctness."""

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import time
import unittest
from unittest.mock import patch

from adapter import Adapter, RequestFailure
from content import bump
from delivery import LogicalFailure, PerformanceIO
from performance import run_window
from report import summarize_logical, summarize_runs
from runtime import Control


class Clock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def monotonic_ns(self):
        return round(self.now * 1e9)

    def sleep(self, seconds):
        self.now += seconds


class DeliveryContractTest(unittest.TestCase):
    def test_matrix_keeps_valid_zero_goodput_and_censored_service_outcomes(self):
        results = [{"status": "measured", "clients": 8, "variant": None,
                    "validated_comparison": True,
                    "logical_summary": {"success_ops_per_second": value, "censored": {"count": 1}}}
                   for value in (0, 0, 12)]
        summary = summarize_runs(results, 3, "baseline", "P2", 3)
        self.assertTrue(summary["all_planned_observations_validated"])
        self.assertNotIn("all_planned_runs_passed", summary)
        self.assertEqual(summary["groups"][0]["median"], 0)
        self.assertEqual(summary["groups"][0]["range"], [0, 12])

    def test_matrix_does_not_select_only_successful_repetitions(self):
        results = [{"status": "measured", "clients": 2, "variant": None,
                    "validated_comparison": True, "logical_summary": {"success_ops_per_second": 10}}
                   for _ in range(3)]
        results[-1]["validated_comparison"] = False
        for observations in (results, results[:2]):
            summary = summarize_runs(observations, 3, "baseline", "P2", 3)
            self.assertFalse(summary["all_planned_observations_validated"])
            self.assertIsNone(summary["groups"][0]["median"])
            self.assertIsNone(summary["groups"][0]["range"])

    def make_io(self, root, clock, statuses, budget=60):
        calls = []

        def send(kind, endpoint, client, request, sql, metadata, **kwargs):
            calls.append((client, request, sql))
            clock.sleep(0.2)
            status = statuses.pop(0)
            return {"event": "return", "status": status, "request_id": request,
                    "committed_request_id": request, "committed_index": 7,
                    "published_applied_index": 7, "payload_hex": "result"}

        base = Adapter(SimpleNamespace(send=send, clock=clock.monotonic_ns, quiesce=lambda _: None), ["node1"])
        base.leader = "node1"

        def discover(deadline):
            clock.sleep(0.3)
            if clock.monotonic() >= deadline:
                raise TimeoutError("discovery deadline")
            base.leader = "node2"
        base.discover = discover
        def check(deadline=None):
            if clock.monotonic() >= min(1000, deadline or 1000):
                raise TimeoutError("observation deadline")
        control = SimpleNamespace(deadline=1000, check=check)
        return PerformanceIO(base, root, {"request_seconds": budget}, control), calls

    def test_retry_uses_same_write_and_counts_backoff_and_discovery(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory, patch("delivery.time", clock):
            root = Path(directory)
            io, calls = self.make_io(root, clock, ["NOT_LEADER"] * 9 + ["COMMITTED"])
            try:
                io.write(bump(7, 1001), 101, "measure", 1000)
            finally:
                io.close()
            result = json.loads((root / "logical_history.jsonl").read_text().splitlines()[-1])
        self.assertEqual(len(set(calls)), 1)
        self.assertEqual(len(calls), 10)  # Time budget, not the correctness suite's eight-attempt ceiling.
        self.assertEqual(result["attempts"], 10)
        self.assertEqual(result["outcome"], "SUCCESS")
        self.assertGreater(result["elapsed_ns"], 2_000_000_000)
        self.assertFalse(result["censored"])

    def test_deadline_keeps_write_identity_and_is_not_exact_success(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory, patch("delivery.time", clock):
            root = Path(directory)
            io, calls = self.make_io(root, clock, ["TIMEOUT"], budget=0.4)
            try:
                with self.assertRaises(LogicalFailure) as caught:
                    io.write(bump(7, 1001), 101, "measure", 1000)
                self.assertEqual(caught.exception.outcome, "UNCONFIRMED")
                with self.assertRaises(RuntimeError):
                    io.write(bump(8, 1002), 101, "measure", 1000)
                self.assertEqual(io.pending[101].request_id, 1)
            finally:
                io.close()
            result = json.loads((root / "logical_history.jsonl").read_text().splitlines()[-1])
        self.assertEqual(len(calls), 1)
        self.assertTrue(result["censored"])
        self.assertEqual(result["elapsed_ns"], 400_000_000)
        self.assertIsNone(result["committed_index"])

    def test_rejection_is_exact_failure_and_malformed_reply_is_not_retried(self):
        for status, outcome, error in (("REJECTED", "FAILED", LogicalFailure),
                                       ("BOGUS", "INVALID_RESPONSE", RequestFailure)):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                clock = Clock()
                with patch("delivery.time", clock):
                    root = Path(directory)
                    io, calls = self.make_io(root, clock, [status])
                    try:
                        with self.assertRaises(error):
                            io.write(bump(7, 1001), 101, "measure", 1000)
                    finally:
                        io.close()
                result = json.loads((root / "logical_history.jsonl").read_text().splitlines()[-1])
                self.assertEqual(len(calls), 1)
                self.assertEqual(result["outcome"], outcome)
                self.assertFalse(result["censored"])
                self.assertEqual(result["elapsed_ns"], 200_000_000)

    def test_unissued_queued_write_does_not_block_session(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory, patch("delivery.time", clock):
            io, calls = self.make_io(Path(directory), clock, ["COMMITTED"])
            try:
                with self.assertRaises(LogicalFailure) as caught:
                    io.write(bump(7, 1001), 101, "foreground", 1000, trace={"planned_python_ns": 1})
                self.assertEqual(caught.exception.outcome, "NOT_ISSUED")
                io.write(bump(8, 1002), 101, "foreground", 1000)
            finally:
                io.close()
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:2], (101, 1))

    def test_censored_and_drain_tail_do_not_inflate_window_goodput(self):
        window = {"phase": "measure", "python_start_ns": 0, "python_end_ns": 180_000_000_000}
        events = []
        for number in range(100):
            start = 179_000_000_000 if number == 97 else 0
            item = {"event": "start", "logical_id": str(number), "phase": "measure",
                    "started_python_ns": start, "deadline_python_ns": start + 60_000_000_000}
            events.append(item)
            duration = 60_000_000_000 if number >= 98 else 2_000_000_000
            events.append({**item, "event": "end", "outcome": "UNCONFIRMED" if number >= 98 else "SUCCESS",
                           "elapsed_ns": duration, "observed_until_python_ns": start + duration, "attempts": 2})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "logical_history.jsonl").write_text("".join(json.dumps(item) + "\n" for item in events))
            result = summarize_logical(root, window)
        self.assertEqual(result["window_successes"], 97)
        self.assertEqual(result["tail_successes"], 1)
        self.assertEqual(result["success_ops_per_second"], 97 / 180)
        self.assertEqual(result["unconfirmed_fraction"], 0.02)
        self.assertEqual(result["successful_operation_latency"]["p99_ms"], 2000)
        self.assertIsNone(result["full_population_p99_ms"])
        self.assertEqual(result["censored"]["min_observed_ms"], 60000)

    def test_all_writers_stalled_still_observes_entire_window(self):
        called = []

        def execute(*args):
            called.append(args)
            raise LogicalFailure("UNCONFIRMED", {"reason": "retained uncertain write"})

        with tempfile.TemporaryDirectory() as directory:
            before = time.monotonic()
            result = run_window(SimpleNamespace(driver=SimpleNamespace(clock=time.monotonic_ns), request_seconds=0.1),
                                Path(directory), Control(3), 1, 0.05, "measure", execute, writes=True)
            elapsed = time.monotonic() - before
        self.assertGreaterEqual(elapsed, 0.05)
        self.assertEqual(len(called), 1)
        self.assertTrue(result["measurement_complete"])
        self.assertEqual(result["successes_by_client"], [0])
        self.assertGreater(result["stalled_client_seconds"]["0"], 0)


if __name__ == "__main__":
    unittest.main()
