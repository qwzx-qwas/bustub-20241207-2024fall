"""Real pipe framing/collection checks; no database workload or production hooks."""

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import unittest

from runtime import Control, Driver


CHILD = '''#!/usr/bin/env python3
import json, sys, time

def request():
    fields = sys.stdin.readline().strip().split("\\t")
    assert fields[2] == "READ" and bytes.fromhex(fields[8]).decode().startswith("SELECT ")
    return {"format_version": 1, "correlation": int(fields[1]), "kind": "READ",
            "client_id": int(fields[3]), "request_id": int(fields[4])}

def record(r, event, **values):
    return (json.dumps({**r, "event": event, **values}, ensure_ascii=False) + "\\n").encode()

def output(data):
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

r = request()
output(record(r, "call", call_ns=time.monotonic_ns()))
if MODE == "truncated":
    output(b'{"format_version":1,"correlation":' + str(r["correlation"]).encode() + b',"rows":[')
    sys.exit(0)
output(record(r, "return", status="OK", rows=[["7", "checkpoint"]]))
# Wait for more input with the pipe still open. The first complete line must
# reach the caller without filling 64 KiB or requiring EOF.
requests = [request(), request()]
for r in requests:
    output(record(r, "call", call_ns=time.monotonic_ns()))
data = b"".join(record(r, "return", status="OK",
                     rows=[["缓存-" * 30000 if r["request_id"] == 201 else "ready"]])
                for r in reversed(requests))
# Split within a UTF-8 character, and combine the remaining records in one
# stream. Correlation, full lines and payload bytes must survive both cases.
split = data.index("缓".encode()) + 1
output(data[:split])
output(data[split:])
assert sys.stdin.read() == ""
'''


def read_calls():
    return dict(line.split(": ") for line in Path("/proc/self/io").read_text().splitlines())["syscr"]


class DriverStreamTest(unittest.TestCase):
    def open_driver(self, root, mode):
        child = root / "pipe-peer"
        child.write_text(CHILD.replace('r = request()\n', f'MODE = {mode!r}\nr = request()\n', 1))
        child.chmod(0o700)
        return Driver(child, root, Control(15))

    def send(self, driver, request_id):
        return driver.send("READ", "127.0.0.1:39101", 11, request_id,
                           "SELECT id, note FROM records WHERE id = 7;", {"phase": "stream_check"})

    def test_live_pipe_fragmented_large_records_and_out_of_order_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            driver = self.open_driver(root, "complete")
            try:
                before = int(read_calls())
                self.assertEqual(self.send(driver, 101)["rows"], [["7", "checkpoint"]])
                with ThreadPoolExecutor(max_workers=2) as workers:
                    wide = workers.submit(self.send, driver, 201)
                    small = workers.submit(self.send, driver, 202)
                    self.assertEqual(wide.result()["rows"], [["缓存-" * 30000]])
                    self.assertEqual(small.result()["rows"], [["ready"]])
                # A generous syscall bound detects the old byte-at-a-time path;
                # it does not assert buffer size, timing, or an exact read count.
                self.assertLess(int(read_calls()) - before, 4096)
            finally:
                cleanup = driver.close()
            self.assertTrue(cleanup["reader_stopped"])
            self.assertEqual(cleanup["exit_code"], 0)
            self.assertIsNone(cleanup["history_error"])
            events = [json.loads(line) for line in (root / "history.jsonl").read_text().splitlines()]
            self.assertEqual(len(events), 6)
            self.assertEqual(sorted(e["request_id"] for e in events if e["event"] == "return"), [101, 201, 202])

    def test_truncated_record_fails_pending_request_and_closes_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            driver = self.open_driver(Path(directory), "truncated")
            try:
                with self.assertRaises(RuntimeError):
                    self.send(driver, 301)
            finally:
                cleanup = driver.close()
            self.assertTrue(cleanup["reader_stopped"])
            self.assertEqual(cleanup["exit_code"], 0)
            self.assertTrue(cleanup["history_error"])


if __name__ == "__main__":
    unittest.main()
