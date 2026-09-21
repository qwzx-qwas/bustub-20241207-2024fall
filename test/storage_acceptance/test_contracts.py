"""Reviewable checks for the harness's own risk boundaries, not production mocks.

Run separately only after review. These do not establish any server correctness.
"""

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from adapter import Adapter, RequestFailure, quote, require_success, retryable
from content import Oracle, Row, bump, c1_deletes, c1_reinserts, c1_updates, fixture, insert_rows, payload_text, read_point
from history import export_history
from faults import Network
from report import summarize
from runtime import Control


GOLDEN_ROW = Row(7, 3, 2, 0, 0, "order=ready|region=east|priority=normal|channel=web|order=ready|")


class HarnessContractTest(unittest.TestCase):
    def test_oracle_rejects_lost_updates_and_duplicate_rows(self):
        operation = bump(7, 1001)
        model = Oracle()
        model.apply(insert_rows([GOLDEN_ROW]))
        model.apply(operation)
        expected = [["7", "3", "2", "1", "1001", "order=ready|region=east|priority=normal|channel=web|order=ready|"]]
        model.check(read_point(7), expected)
        with self.assertRaises(AssertionError):
            model.check(read_point(7), expected + expected)
        with self.assertRaises(AssertionError):
            model.check(read_point(7), [["7", "3", "2", "0", "0", GOLDEN_ROW.payload]])

    def test_fixture_and_payload_literal_preserve_bytes(self):
        text = "customer's order\tready\nnext"
        # Protect literal data escaping, not an UPDATE's spelling, whitespace or
        # assignment order. SQL execution is checked through the real C1 path.
        self.assertEqual(quote(text), "'customer''s order\tready\nnext'")
        # Small literal generated-value example, independent of randomized fixture ordering.
        self.assertEqual(payload_text(64, 0), GOLDEN_ROW.payload)

    def test_c1_exercises_growth_shrinkage_and_fixed_final_cardinality(self):
        rows = fixture(2048)
        model = Oracle()
        model.apply(insert_rows(rows))
        grew = shrank = False
        for operation in c1_updates(rows):
            before = len(model.rows[operation.args["id"]].payload)
            after = len(operation.args["payload"])
            grew |= after > before
            shrank |= after < before
            self.assertIn(after, (64, 256, 1024))
            model.apply(operation)
        self.assertTrue(grew and shrank)
        for operation in c1_deletes(rows):
            model.apply(operation)
        model.apply(insert_rows(list(c1_reinserts(rows))))
        self.assertEqual(len(model.rows), 1792)

    def test_unknown_write_cannot_advance_client(self):
        class RecordedFailure:
            def __init__(self):
                self.calls = []

            def send(self, *args, **kwargs):
                self.calls.append((args, kwargs))
                return {"event": "return", "status": "TIMEOUT", "request_id": 1}

        driver = RecordedFailure()
        adapter = Adapter(driver, ["127.0.0.1:32101"])
        adapter.leader = "127.0.0.1:32101"
        with self.assertRaises(RequestFailure) as caught:
            adapter.write(bump(7, 1001), 101, "measure", deadline=10**20)
        self.assertEqual(caught.exception.outcome, "UNKNOWN")
        with self.assertRaises(RuntimeError):
            adapter.write(bump(8, 1002), 101, "measure", deadline=10**20)
        self.assertEqual(len(driver.calls), 1)
        self.assertEqual(driver.calls[0][0][2:4], (101, 1))

    def test_retry_retains_identity_and_original_sql_bytes(self):
        class Replies:
            def __init__(self):
                self.calls = []

            def send(self, *args, **kwargs):
                self.calls.append(args)
                if len(self.calls) == 1:
                    return {"event": "return", "status": "TIMEOUT", "request_id": 1}
                return {"event": "return", "status": "COMMITTED", "request_id": 1,
                        "committed_request_id": 1, "committed_index": 5,
                        "published_applied_index": 5, "payload_hex": "retained-result"}

        driver = Replies()
        adapter = Adapter(driver, ["127.0.0.1:32101"])
        intent = adapter.begin_write(bump(7, 1001), 101)
        with self.assertRaises(RequestFailure):
            adapter.attempt_write(intent, "history", 10**20, endpoint="127.0.0.1:32101")
        adapter.attempt_write(intent, "history", 10**20, endpoint="127.0.0.1:32102")
        self.assertEqual(driver.calls[0][2:5], driver.calls[1][2:5])
        self.assertEqual(adapter.begin_write(bump(7, 1002), 101).request_id, 2)

    def test_malformed_response_is_not_retried_as_transport_loss(self):
        lost = RequestFailure("UNKNOWN", {"event": "error",
                                           "error": "distributed client response prefix is unavailable"})
        malformed = RequestFailure("UNKNOWN", {"event": "error",
                                                "error": "distributed client response request ID does not match request"})
        self.assertTrue(retryable(lost))
        self.assertFalse(retryable(malformed))
        for status in ("OK", "UNRECOGNIZED", None):
            with self.subTest(status=status), self.assertRaises(RequestFailure) as caught:
                require_success({"event": "return", "status": status}, write=True)
            self.assertEqual(caught.exception.outcome, "INVALID_RESPONSE")

    def test_leadership_loss_does_not_erase_a_possibly_proposed_write(self):
        operation = bump(7, 1001)
        item = {"correlation": 1, "kind": "WRITE", "phase": "history", "operation": operation.record(),
                "client_id": 101, "request_id": 1, "sql": "UPDATE original bytes"}
        history = [{"correlation": 1, "event": "call", "call_ns": 10},
                   {"correlation": 1, "event": "return", "call_ns": 10, "return_ns": 20,
                    "status": "NOT_LEADER"}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "history_initial.json").write_text(json.dumps(GOLDEN_ROW.values()))
            (root / "history_boundary.json").write_text(json.dumps({"end_ns": 30}))
            for name, values in (("inputs.jsonl", [item]), ("history.jsonl", history)):
                (root / name).write_text("".join(json.dumps(value) + "\n" for value in values))
            export_history(root)
            attempt = json.loads((root / "linearizability.json").read_text())["attempts"][0]
        self.assertEqual(attempt["outcome"], "UNKNOWN")
        self.assertEqual(attempt["return_ns"], 20)  # Raw observation is not rewritten.

    def test_snapshot_evidence_keeps_real_install_and_rejects_stale_only(self):
        request = {"event": "forwarded", "monotonic_ns": 10, "type": 5, "from": 1, "to": 2,
                   "term": 3, "request_id": 9, "done": True, "last_included_index": 100,
                   "total_size": 1024}
        installed = {"event": "forwarded", "monotonic_ns": 20, "type": 6, "from": 2, "to": 1,
                     "term": 3, "request_id": 9, "success": True, "complete": True, "stale": False,
                     "match_index": 100, "next_offset": 0}
        duplicate = {**installed, "monotonic_ns": 30, "stale": True}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trace = root / "nodes/proxy-2/events.jsonl"
            trace.parent.mkdir(parents=True)
            trace.write_text("".join(json.dumps(value) + "\n" for value in (request, installed, duplicate)))
            control = Control(1)
            network = Network(SimpleNamespace(root=root), control)
            evidence = network.catchup(2, "long", 0, 50, control.deadline)
            self.assertFalse(evidence["response"]["stale"])
            trace.write_text("".join(json.dumps(value) + "\n" for value in (request, duplicate)))
            control = Control(0.05)
            with self.assertRaises(TimeoutError):
                Network(SimpleNamespace(root=root), control).catchup(2, "long", 0, 50, control.deadline)

    def test_window_throughput_excludes_tail_and_keeps_missing_return(self):
        inputs = [{"correlation": i, "kind": "WRITE", "phase": "measure"} for i in (1, 2, 3)]
        def call(i, at):
            return {"correlation": i, "event": "call", "call_ns": at}

        def returned(i, at, done):
            return {"correlation": i, "event": "return", "call_ns": at, "return_ns": done,
                    "status": "COMMITTED", "request_id": 1, "committed_request_id": 1,
                    "committed_index": i, "published_applied_index": i}

        history = [call(1, 1000000000), returned(1, 1000000000, 1500000000),
                   call(2, 2900000000), call(3, 2950000000), returned(2, 2900000000, 3100000000)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, values in (("inputs.jsonl", inputs), ("history.jsonl", history)):
                (root / name).write_text("".join(json.dumps(value) + "\n" for value in values))
            result = summarize(root, {"start_ns": 1000000000, "end_ns": 3000000000})
        self.assertEqual(result["window_calls"], 3)
        self.assertEqual(result["window_successes"], 1)
        self.assertEqual(result["tail_successes"], 1)
        self.assertEqual(result["success_ops_per_second"], 0.5)
        self.assertEqual(result["all_attempt_outcomes"]["UNKNOWN"], 1)
        self.assertEqual(result["successful_call_latency"]["count"], 2)
        self.assertEqual(result["successful_call_latency"]["p99_ms"], 500)


if __name__ == "__main__":
    unittest.main()
