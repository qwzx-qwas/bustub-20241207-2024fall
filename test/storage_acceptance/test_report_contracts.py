"""Offline reporting contracts; no nodes, network, or production hooks."""

import json
from pathlib import Path
import tempfile
import unittest

from report import summarize_logical


class LogicalReportTest(unittest.TestCase):
    def history(self, root, requests):
        events = []
        for number, (phase, start, end, deadline, outcome, attempts) in enumerate(requests):
            item = {"logical_id": str(number), "phase": phase,
                    "started_python_ns": start * 10**9, "deadline_python_ns": deadline * 10**9}
            events.extend([{**item, "event": "start"},
                           {**item, "event": "end", "outcome": outcome, "attempts": attempts,
                            "observed_until_python_ns": end * 10**9, "elapsed_ns": (end - start) * 10**9}])
        (root / "logical_history.jsonl").write_text("".join(json.dumps(x) + "\n" for x in events))

    def test_turnover_keeps_timeout_excludes_reconcile_and_counts_idle_time(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "measurement_windows.json").write_text(json.dumps([
                {"phase": "turnover_1", "python_start_ns": 100 * 10**9, "python_end_ns": 200 * 10**9},
                {"phase": "turnover_2", "python_start_ns": 300 * 10**9, "python_end_ns": 400 * 10**9}]))
            (root / "turnover_progress.json").write_text(json.dumps({"observed_seconds": 600}))
            self.history(root, [("load", 1, 2, 61, "SUCCESS", 1),
                                ("turnover_1", 110, 115, 170, "SUCCESS", 2),
                                ("verify_round_1", 205, 207, 265, "SUCCESS", 1),
                                ("turnover_2", 310, 312, 370, "SUCCESS", 1),
                                ("turnover_2", 330, 390, 390, "UNCONFIRMED", 3),
                                ("reconcile", 450, 455, 510, "SUCCESS", 1)])
            result = summarize_logical(root, None)
        self.assertEqual(result["logical_operations"], 3)
        self.assertEqual(result["outcomes"], {"SUCCESS": 2, "UNCONFIRMED": 1})
        self.assertEqual(result["retry_attempts"], 3)
        self.assertEqual(result["deadline_misses"], 1)
        self.assertEqual(result["censored"]["min_observed_ms"], 60000)
        self.assertIsNone(result["full_population_p99_ms"])
        self.assertEqual(result["window_seconds"], 600)
        self.assertAlmostEqual(result["success_ops_per_second"], 2 / 600)

    def test_single_window_keeps_drain_tail_out_of_goodput(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.history(root, [("measure", 105, 109, 165, "SUCCESS", 1),
                                ("measure", 109, 111, 169, "SUCCESS", 1),
                                ("measure", 104, 110, 110, "UNCONFIRMED", 2)])
            result = summarize_logical(root, {"phase": "measure", "python_start_ns": 100 * 10**9,
                                               "python_end_ns": 110 * 10**9})
        self.assertEqual(result["window_successes"], 1)
        self.assertEqual(result["tail_successes"], 1)
        self.assertEqual(result["deadline_misses"], 1)
        self.assertAlmostEqual(result["success_ops_per_second"], 0.1)


if __name__ == "__main__":
    unittest.main()
