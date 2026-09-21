"""Independent checks of workload/measurement promises, not database substitutes."""

from collections import Counter
import itertools
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest

from arrival import ArrivalTraffic
from content import Oracle, bump, fixture, insert_rows, p1_operations, p3_groups, p4_operations, read_point
from performance import apply_foreground
from report import summarize_arrivals, summarize_space, trend
from runtime import Control


class PerformanceContractTest(unittest.TestCase):
    def test_read_stream_exercises_shared_hot_keys_and_rest_of_table(self):
        def keys(distribution):
            return [op.args["id"] for op in itertools.islice(p1_operations(2000, 0, 20260920, distribution, "measure"), 2000)]
        hot, uniform = keys("hotspot"), keys("uniform")
        self.assertTrue(all(0 <= key < 2000 for key in hot + uniform))
        self.assertGreater(sum(key < 100 for key in hot), 1500)
        self.assertLess(sum(key < 100 for key in hot), 1700)
        self.assertGreater(len(set(uniform)), 1000)
        self.assertLess(len(set(hot)), len(set(uniform)))
        self.assertEqual(hot, keys("hotspot"))

    def test_turnover_preserves_live_bytes_and_rows_after_each_real_group(self):
        rows = fixture(100)
        state = {row.id: row.payload for row in rows}
        expected_bytes = sum(map(len, state.values()))
        rounds = Counter()
        for round_number, operations in p3_groups(rows, 20260920, rounds=2):
            self.assertEqual(Counter(op.name for op in operations), {"ReplacePayload": 14, "Delete": 3, "InsertRows": 3})
            touched = set()
            grew = shrank = 0
            for op in operations:
                args = op.args
                if op.name == "ReplacePayload":
                    key = args["id"]
                    self.assertNotIn(key, touched)
                    touched.add(key)
                    delta = len(args["payload"]) - len(state[key])
                    grew += delta > 0
                    shrank += delta < 0
                    state[key] = args["payload"]
                elif op.name == "Delete":
                    self.assertNotIn(args["id"], touched)
                    touched.add(args["id"])
                    del state[args["id"]]
                else:
                    self.assertEqual(len(args["rows"]), 1)
                    row = args["rows"][0]
                    self.assertNotIn(row["id"], state)
                    state[row["id"]] = row["payload"]
            self.assertEqual((grew, shrank), (7, 7))
            self.assertEqual((len(state), sum(map(len, state.values()))), (100, expected_bytes))
            rounds[round_number] += len(operations)
        self.assertEqual(rounds, {1: 100, 2: 100})

    def test_recovery_traffic_has_fixed_mix_and_disjoint_writers(self):
        items = list(itertools.islice(p4_operations(100, 20260920), 30))
        self.assertEqual([item[0] for item in items], list(range(1, 31)))
        for offset in range(0, 30, 10):
            block = items[offset:offset + 10]
            self.assertEqual(Counter(op.name for _, _, op in block), {"ReadPoint": 8, "Bump": 2})
            for _, writer, op in block:
                self.assertTrue(op.args)
                if writer is not None:
                    self.assertTrue(writer * 50 <= op.args["id"] < (writer + 1) * 50)

    def test_space_growth_is_not_reported_as_steady(self):
        self.assertEqual(trend([10000, 10010, 10000, 10010, 10000])["status"], "stable_window")
        self.assertEqual(trend([10000, 20000, 30000, 40000, 50000])["status"], "observed_growth_or_variation")
        self.assertEqual(trend([0] * 5)["status"], "insufficient_samples")
        self.assertEqual(trend([10000] * 4)["status"], "insufficient_samples")

    def test_flat_space_without_retention_cycles_is_not_steady_evidence(self):
        rounds, samples = [], []
        for number in range(1, 11):
            usage = {"kind": "filesystem_allocated_blocks", "total_bytes": 10000}
            rounds.append({"round": number, "phase": f"turnover_{number}", "start_ns": number * 100,
                           "end_ns": number * 100 + 50, "python_start_ns": number * 100,
                           "python_end_ns": number * 100 + 50, "before": usage, "after": usage,
                           "observed_retention_advances": min(number, 3)})
            samples.append({"monotonic_ns": number * 100 + 25, "storage_usage": usage})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, values in (("space_rounds.jsonl", rounds), ("resources.jsonl", samples),
                                 ("inputs.jsonl", []), ("history.jsonl", [])):
                (root / name).write_text("".join(json.dumps(value) + "\n" for value in values))
            self.assertEqual(summarize_space(root)["status"], "not_covered")

    def test_foreground_read_cannot_mix_version_and_operation_number(self):
        rows = fixture(100)
        inputs = [{"correlation": 1, "kind": "WRITE", "phase": "foreground", "operation": bump(7, 1001).record()},
                  {"correlation": 2, "kind": "READ", "phase": "foreground", "operation": read_point(7).record()}]
        observed = rows[7].values()
        observed[3:5] = ["1", "0"]  # New version with old last_op is not a reachable row.
        events = [{"correlation": 1, "event": "return", "status": "COMMITTED", "request_id": 1,
                   "committed_request_id": 1, "committed_index": 5, "published_applied_index": 5},
                  {"correlation": 2, "event": "return", "status": "OK", "rows": [observed]}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "inputs.jsonl").write_text("".join(json.dumps(value) + "\n" for value in inputs))
            (root / "history.jsonl").write_text("".join(json.dumps(value) + "\n" for value in events))
            oracle = Oracle()
            oracle.apply(insert_rows(rows))
            with self.assertRaises(AssertionError):
                apply_foreground(root, oracle)

    def test_arrival_metrics_include_queue_delay_drop_and_drain_tail(self):
        timeline = {"start_ns": 1000000000, "stop_follower_ns": 2000000000,
                    "restart_issued_ns": 3000000000, "recovered_k0_ns": 4000000000, "end_ns": 5000000000}
        inputs = [{"correlation": 1, "kind": "READ", "phase": "foreground", "planned_ns": 1500000000}]
        events = [{"correlation": 1, "event": "call", "call_ns": 2100000000},
                  {"correlation": 1, "event": "return", "status": "OK", "call_ns": 2100000000,
                   "return_ns": 5500000000}]
        plans = [{"planned_ns": 1500000000, "admitted": True}, {"planned_ns": 1600000000, "admitted": False}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "recovery_timeline.json").write_text(json.dumps(timeline))
            (root / "arrival_dispatch.json").write_text(json.dumps({"queue_rejections": 1}))
            for name, values in (("inputs.jsonl", inputs), ("history.jsonl", events), ("arrivals.jsonl", plans)):
                (root / name).write_text("".join(json.dumps(value) + "\n" for value in values))
            result = summarize_arrivals(root)["windows"]
        healthy = result["healthy"]
        self.assertEqual(healthy["planned"], 2)
        self.assertEqual(healthy["outcomes"], {"SUCCESS": 1, "NOT_ISSUED_QUEUE_FULL": 1})
        self.assertEqual(healthy["latencies"]["queue"]["p50_ms"], 600)
        self.assertEqual(healthy["latencies"]["service_success"]["p50_ms"], 3400)
        self.assertEqual(healthy["latencies"]["schedule_success"]["p50_ms"], 4000)
        self.assertEqual(sum(window["completed_in_window"] for window in result.values()), 0)

    def test_slow_delivery_preserves_arrivals_and_bounded_queue(self):
        rows, release = fixture(100), threading.Event()

        class Delivery:
            driver = SimpleNamespace(clock=time.monotonic_ns)

            def read(self, operation, *args, **kwargs):
                if not release.wait(timeout=2):
                    raise TimeoutError("self-check delivery was not released")
                return [rows[operation.args["id"]].values()]

            def write(self, operation, *args, **kwargs):
                if not release.wait(timeout=2):
                    raise TimeoutError("self-check delivery was not released")
                return {"committed_index": operation.args["last_op"]}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            traffic = ArrivalTraffic(Delivery(), rows, root, {"rate": 10000, "seed": 20260920}, Control(3))
            try:
                traffic.start()
                deadline = time.monotonic() + 1
                while traffic.rejected == 0:
                    traffic.check(deadline)
                    time.sleep(0.001)
            finally:
                traffic.stopping.set()
                release.set()
                traffic.close()
            traffic.check()
            record = json.loads((root / "arrival_dispatch.json").read_text())
            plans = [json.loads(line) for line in (root / "arrivals.jsonl").read_text().splitlines()]
        self.assertEqual(record["max_queued"], 64)
        self.assertLessEqual(record["max_inflight"], 6)
        self.assertTrue(record["all_workers_stopped"])
        self.assertGreater(record["queue_rejections"], 0)
        self.assertEqual(record["generated_arrivals"], len(plans))
        self.assertEqual(record["queue_rejections"], sum(not plan["admitted"] for plan in plans))
        self.assertEqual(record["queued_unissued"], 0)


if __name__ == "__main__":
    unittest.main()
