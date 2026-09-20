#!/usr/bin/env python3
"""Repeatable, single-host baseline against the production Raft client protocol.

Uses new directories, owns its child processes, and never changes storage code.
This is a closed-loop, concurrency-one development baseline, not a load generator
for maximum throughput or a power-loss durability test.
"""

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import socket
import struct
import subprocess
import time
import traceback


def crc32c(data):
    value = 0xFFFFFFFF
    for byte in data:
        value = CRC_TABLE[(value ^ byte) & 255] ^ (value >> 8)
    return value ^ 0xFFFFFFFF


CRC_TABLE = []
for _value in range(256):
    for _ in range(8):
        _value = (_value >> 1) ^ (0x82F63B78 if _value & 1 else 0)
    CRC_TABLE.append(_value)
assert crc32c(b"123456789") == 0xE3069283


class Reader:
    def __init__(self, data):
        self.data, self.offset = data, 0

    def take(self, count):
        result = self.data[self.offset:self.offset + count]
        if len(result) != count:
            raise ValueError("short protocol field")
        self.offset += count
        return result

    def number(self, fmt):
        return struct.unpack(">" + fmt, self.take(struct.calcsize(">" + fmt)))[0]

    def string(self):
        return self.take(self.number("I")).decode()

    def end(self):
        if self.offset != len(self.data):
            raise ValueError("trailing protocol bytes")


def wire_string(value):
    data = value.encode()
    return struct.pack(">I", len(data)) + data


def receive(sock, size):
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise EOFError("connection closed before response completed")
        chunks.extend(chunk)
    return bytes(chunks)


def call(port, payload):
    frame = b"BCLNT001" + struct.pack(">II", 1, len(payload)) + payload
    frame += struct.pack(">I", crc32c(payload))
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.sendall(frame)
        header = receive(sock, 16)
        version, size = struct.unpack(">II", header[8:])
        if header[:8] != b"BCLNT001" or version != 1 or size > 16 * 1024**2:
            raise ValueError("invalid response frame")
        body = receive(sock, size)
        if struct.unpack(">I", receive(sock, 4))[0] != crc32c(body):
            raise ValueError("response checksum mismatch")
    reader = Reader(body)
    if reader.number("I") != 4:
        raise ValueError("expected response frame")
    result = {"request_id": reader.number("Q"), "status": reader.number("I"),
              "node_id": reader.number("Q"), "leader_ready": reader.number("B")}
    result["leader_id"] = reader.number("Q") if reader.number("B") else None
    result["leader_address"] = reader.string()
    for name in ("term", "commit_index", "last_applied", "published_applied_index", "snapshot_base_index"):
        result[name] = reader.number("Q")
    result["read_timestamp"] = reader.number("Q") if reader.number("B") else None
    result["payload"] = reader.take(reader.number("I"))
    reader.end()
    return result


def query_rows(payload):
    if payload[:8] != b"BQRES001" or crc32c(payload[8:-4]) != struct.unpack(">I", payload[-4:])[0]:
        raise ValueError("invalid query result")
    reader = Reader(payload[8:-4])
    if reader.number("I") != 1:
        raise ValueError("unsupported result version")
    columns = [reader.string() for _ in range(reader.number("I"))]
    rows = [[reader.string() for _ in columns] for _ in range(reader.number("I"))]
    reader.end()
    return rows


def command_output(args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def space(root):
    groups = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            info = path.stat()
        except FileNotFoundError:
            continue  # A snapshot may be retired while sampling.
        rel = path.relative_to(root)
        key = "/".join(rel.parts[:3])
        item = groups.setdefault(key, {"logical_bytes": 0, "allocated_bytes": 0, "files": 0})
        item["logical_bytes"] += info.st_size
        item["allocated_bytes"] += info.st_blocks * 512
        item["files"] += 1
    return groups


class Baseline:
    def __init__(self, args):
        self.args, self.processes, self.logs = args, {}, []
        self.write_id, self.read_id = 0, 1000000
        self.expected = {i: 0 for i in range(args.rows)}
        self.rng = random.Random(args.seed)
        self.leader = None
        self.records, self.phases, self.recovery = [], [], []
        self.padding = "x" * 256

    def port(self, node):
        return self.args.port_base + 100 + node

    def start(self, node):
        args = [str(self.args.build_dir / "bin/bustub-node"), "--node-id", str(node),
                "--group-id", "storage-baseline", "--data-dir", str(self.args.runtime_dir / f"node-{node}"),
                "--raft-listen", f"127.0.0.1:{self.args.port_base + node}",
                "--client-listen", f"127.0.0.1:{self.port(node)}",
                "--buffer-pool-size", "32", "--snapshot-threshold-entries", str(self.args.snapshot_threshold),
                "--election-timeout-min-ms", "800", "--election-timeout-max-ms", "1600"]
        for peer in range(1, 4):
            if peer != node:
                args += ["--peer", f"{peer}=127.0.0.1:{self.args.port_base + peer},127.0.0.1:{self.port(peer)}"]
        log = (self.args.runtime_dir / f"node-{node}.log").open("ab")
        self.logs.append(log)
        self.processes[node] = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)

    def stop(self, node):
        process = self.processes.pop(node)
        if process.poll() is None:
            process.kill()
        process.wait(timeout=20)

    def status(self, node):
        return call(self.port(node), struct.pack(">IQ", 3, 90000000))

    def find_leader(self):
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            for node, process in self.processes.items():
                if process.poll() is not None:
                    raise RuntimeError(f"node {node} exited: {self.args.runtime_dir}/node-{node}.log")
                try:
                    status = self.status(node)
                    if status["leader_ready"]:
                        self.leader = node
                        return node
                except (OSError, EOFError):
                    pass
            time.sleep(0.05)
        raise TimeoutError("no ready leader")

    def write(self, sql):
        self.write_id += 1
        result = call(self.port(self.leader), struct.pack(">IQQ", 1, 902, self.write_id) + wire_string(sql))
        if result["status"] != 1 or result["request_id"] != self.write_id:
            raise RuntimeError(f"write failed: {result}")
        if len(result["payload"]) != 32:
            raise ValueError("invalid write acknowledgment")
        version, status, request_id, term, index = struct.unpack(">IIQQQ", result["payload"])
        if (version, status, request_id) != (1, 1, self.write_id):
            raise ValueError("write acknowledgment identity mismatch")
        return index

    def read(self, sql, node=None, stale=False):
        self.read_id += 1
        result = call(self.port(node or self.leader), struct.pack(">IQI", 2, self.read_id, 2 if stale else 1)
                      + wire_string(sql))
        if result["status"] != 2 or result["request_id"] != self.read_id:
            raise RuntimeError(f"read failed: {result}")
        return query_rows(result["payload"])

    def verify(self, node=None):
        rows = self.read("SELECT id, value FROM baseline ORDER BY id;", node, node is not None)
        expected = [[str(key), str(value)] for key, value in sorted(self.expected.items())]
        if rows != expected:
            raise AssertionError(f"state mismatch at node {node}: {len(rows)} rows, expected {len(expected)}")

    def await_index(self, node, index, snapshot_after=None):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                status = self.status(node)
                if status["published_applied_index"] >= index and (
                        snapshot_after is None or status["snapshot_base_index"] > snapshot_after):
                    return {key: value for key, value in status.items() if key != "payload"}
            except (OSError, EOFError):
                pass
            time.sleep(0.02)
        raise TimeoutError(f"node {node} did not reach index {index}")

    def sample(self):
        result = {"space": space(self.args.runtime_dir), "processes": {}}
        for node, process in self.processes.items():
            proc = Path(f"/proc/{process.pid}")
            try:
                status_lines = (proc / "status").read_text().splitlines()
                result["processes"][node] = {
                    "io": (proc / "io").read_text(), "stat": (proc / "stat").read_text(),
                    "memory": [line for line in status_lines if line.startswith(("VmRSS:", "VmHWM:"))],
                    "raft": {k: v for k, v in self.status(node).items() if k != "payload"}}
            except (OSError, EOFError):
                result["processes"][node] = {"sample_unavailable": True}
        return result

    def phase(self, name, operations):
        before = self.sample()
        start = time.perf_counter()
        records = []
        for operation, key in operations:
            tick = time.perf_counter_ns()
            error = ""
            try:
                if operation == "read":
                    rows = self.read(f"SELECT value FROM baseline WHERE id = {key};")
                    if rows != [[str(self.expected[key])]]:
                        raise AssertionError(f"point read mismatch: {key}: {rows}")
                elif operation == "update":
                    self.write(f"UPDATE baseline SET value = value + 1 WHERE id = {key};")
                    self.expected[key] += 1
                elif operation == "delete":
                    self.write(f"DELETE FROM baseline WHERE id = {key};")
                    del self.expected[key]
                elif operation == "insert":
                    self.write(f"INSERT INTO baseline VALUES ({key}, 0, '{self.padding}');")
                    self.expected[key] = 0
                else:
                    raise ValueError(operation)
            except Exception as exc:
                error = repr(exc)
                raise
            finally:
                row = {"phase": name, "operation": operation, "key": key,
                       "latency_ms": (time.perf_counter_ns() - tick) / 1e6, "success": not error, "error": error}
                records.append(row)
                self.records.append(row)
        elapsed = time.perf_counter() - start
        self.verify()
        latencies = sorted(row["latency_ms"] for row in records)
        percentile = lambda p: latencies[max(0, math.ceil(len(latencies) * p) - 1)]
        result = {"name": name, "operations": len(records), "seconds": elapsed,
                  "successful_ops_per_second": len(records) / elapsed, "errors": 0,
                  "p50_ms": percentile(.5), "p95_ms": percentile(.95), "p99_ms": percentile(.99),
                  "before": before, "after": self.sample()}
        self.phases.append(result)
        print(f"{name}: {result['successful_ops_per_second']:.1f} ops/s, p99={result['p99_ms']:.2f} ms", flush=True)

    def run(self):
        for node in range(1, 4):
            self.start(node)
        self.find_leader()
        # Independently confirm framing against the production CLI.
        cli = command_output([str(self.args.build_dir / "bin/bustub-client"), "status", "--endpoint",
                              f"127.0.0.1:{self.port(self.leader)}", "--request-id", "777"])
        if "leader_ready=1" not in cli:
            raise AssertionError(f"CLI disagrees with leader discovery: {cli}")
        self.write("CREATE TABLE baseline(id int PRIMARY KEY, value int, padding varchar(512));")
        for start in range(0, self.args.rows, 100):
            values = ",".join(f"({key}, 0, '{self.padding}')" for key in range(start, min(start + 100, self.args.rows)))
            self.write("INSERT INTO baseline VALUES " + values + ";")
        for _ in range(100):
            self.read(f"SELECT value FROM baseline WHERE id = {self.rng.randrange(self.args.rows)};")
        count = self.args.operations
        for name, write_fraction, hot in (("point_read_uniform", 0, False), ("point_update_uniform", 1, False),
                                          ("mixed_95r_5w", .05, False), ("mixed_50r_50w_hot", .5, True)):
            operations = [("update" if i < round(count * write_fraction) else "read",
                           self.rng.randrange(max(1, self.args.rows // 100) if hot else self.args.rows))
                          for i in range(count)]
            self.rng.shuffle(operations)
            self.phase(name, operations)
        for round_id in range(3):
            self.phase(f"update_churn_{round_id + 1}", [("update", self.rng.randrange(self.args.rows)) for _ in range(count)])
        keys = list(range(min(count, self.args.rows)))
        self.phase("delete", [("delete", key) for key in keys])
        self.phase("reinsert", [("insert", key) for key in keys])
        # Force the missing interval beyond retained snapshot generations.
        lagger = self.leader % 3 + 1
        old_base = self.status(lagger)["snapshot_base_index"]
        self.stop(lagger)
        self.phase("writes_with_follower_down", [("update", self.rng.randrange(self.args.rows))
                                                 for _ in range(self.args.snapshot_threshold * 4)])
        target = self.status(self.leader)["commit_index"]
        self.await_index(self.leader, target, old_base)
        started = time.perf_counter()
        self.start(lagger)
        status = self.await_index(lagger, target, old_base)
        elapsed = time.perf_counter() - started
        self.verify(lagger)
        self.recovery.append({"name": "follower_restart_and_catchup", "seconds": elapsed, "target": target,
                              "previous_snapshot_base": old_base, "status": status, "verified": True})
        old_leader = self.leader
        started = time.perf_counter()
        self.stop(old_leader)
        self.find_leader()
        self.verify()
        self.recovery.append({"name": "leader_kill_to_ready_and_verified_read",
                              "seconds": time.perf_counter() - started, "verified": True})
        self.start(old_leader)
        target = self.status(self.leader)["commit_index"]
        for node in range(1, 4):
            self.await_index(node, target)
            self.verify(node)
        for node in list(self.processes):
            self.stop(node)
        started = time.perf_counter()
        for node in range(1, 4):
            self.start(node)
        self.find_leader()
        self.verify()
        self.recovery.append({"name": "all_processes_killed_then_restarted_to_verified_read",
                              "seconds": time.perf_counter() - started, "verified": True,
                              "note": "OS cache retained; not a power-loss or cold-device test"})
        target = self.status(self.leader)["commit_index"]
        for node in range(1, 4):
            self.await_index(node, target)
            self.verify(node)
        self.final_sample = self.sample()

    def close(self):
        for node in list(self.processes):
            self.stop(node)
        for log in self.logs:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--port-base", type=int, default=38100)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--operations", type=int, default=500)
    parser.add_argument("--snapshot-threshold", type=int, default=64)
    parser.add_argument("--seed", type=int, default=20260915)
    args = parser.parse_args()
    if min(args.rows, args.operations, args.snapshot_threshold) < 1 or not 1024 <= args.port_base <= 65000:
        parser.error("positive workload sizes and a port base in [1024, 65000] required")
    for name in ("build_dir", "runtime_dir", "report_dir"):
        setattr(args, name, getattr(args, name).resolve())
    for binary in ("bustub-node", "bustub-client"):
        if not (args.build_dir / "bin" / binary).is_file():
            parser.error(f"missing binary: {binary}")
    args.runtime_dir.mkdir(parents=True, exist_ok=False)
    args.report_dir.mkdir(parents=True, exist_ok=False)
    repo = Path(__file__).resolve().parents[2]
    cache = (args.build_dir / "CMakeCache.txt").read_text()
    metadata = {"config": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "utc_start": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "git_head": command_output(["git", "rev-parse", "HEAD"], repo),
                "git_status": command_output(["git", "status", "--short"], repo),
                "tracked_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=repo)).hexdigest(),
                "node_binary_sha256": hashlib.sha256((args.build_dir / "bin/bustub-node").read_bytes()).hexdigest(),
                "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "platform": platform.platform(), "logical_cpus": os.cpu_count(),
                "cpu_model": next((s for s in Path("/proc/cpuinfo").read_text().splitlines() if s.startswith("model name")), ""),
                "meminfo": Path("/proc/meminfo").read_text(),
                "cmake": [line for line in cache.splitlines() if line.startswith(("CMAKE_BUILD_TYPE:", "CMAKE_CXX_COMPILER:"))],
                "compiler": command_output(["g++", "--version"]).splitlines()[0],
                "scope": "single host, 3 processes, loopback, closed loop concurrency=1, OS cache retained",
                "durability": "production Raft synchronous persistence; page stream flush alone is not fsync",
                "not_measured": ["physical device write amplification", "power-loss/torn writes", "true cold cache",
                                 "high disk occupancy", "maximum throughput/load sweep", "multi-host network",
                                 "long-duration steady state", "new FS GC/deferred paths", "isolated 4 KiB device IO"]}
    (args.report_dir / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    baseline = Baseline(args)
    error = None
    try:
        baseline.run()
    except Exception:
        error = traceback.format_exc()
        raise
    finally:
        try:
            baseline.close()
        finally:
            with (args.report_dir / "operations.csv").open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=["phase", "operation", "key", "latency_ms", "success", "error"])
                writer.writeheader()
                writer.writerows(baseline.records)
            report = {"passed": error is None, "error": error, "phases": baseline.phases,
                      "recovery": baseline.recovery, "final": getattr(baseline, "final_sample", None)}
            (args.report_dir / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"Baseline passed; report: {args.report_dir}", flush=True)


if __name__ == "__main__":
    main()
