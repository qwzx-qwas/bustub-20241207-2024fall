"""Test-owned processes, bounded bridge calls, and observable resource budgets."""

from concurrent.futures import Future, TimeoutError as FutureTimeout
import json
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import threading
import time


GIB = 1024**3


def write_json(path, value):
    with path.open("w", encoding="utf-8") as output:
        json.dump(value, output, ensure_ascii=True, sort_keys=True, indent=2)
        output.write("\n")


class EnvironmentLimit(RuntimeError):
    pass


class Control:
    def __init__(self, seconds):
        self.deadline = time.monotonic() + seconds
        self.failure = None
        self._lock = threading.Lock()

    def fail(self, reason):
        with self._lock:
            if self.failure is None:
                self.failure = reason

    def check(self, deadline=None):
        if self.failure:
            raise EnvironmentLimit(self.failure)
        if time.monotonic() >= min(self.deadline, deadline or self.deadline):
            raise TimeoutError("absolute scenario/request observation deadline exceeded")


class Driver:
    def __init__(self, binary, root, control):
        self.control = control
        self._stderr = (root / "driver.stderr").open("xb")
        self._inputs = (root / "inputs.jsonl").open("x", encoding="utf-8", buffering=1)
        self._events = (root / "history.jsonl").open("x", encoding="utf-8", buffering=1)
        self._send_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(8)
        self._pending = {}
        self._next = 0
        self._history_attempts = 0
        self._broken = None
        self._closing = False
        self._reader = threading.Thread(target=self._read, name="bridge-reader", daemon=True)
        self.process = None
        try:
            self.process = subprocess.Popen([str(binary), "8"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                            stderr=self._stderr, bufsize=0)
            os.set_blocking(self.process.stdin.fileno(), False)
            self._reader.start()
        except Exception:
            if self.process:
                self.process.kill()
                self.process.wait(timeout=5)
                self.process.stdin.close()
                self.process.stdout.close()
            self._inputs.close()
            self._events.close()
            self._stderr.close()
            raise

    def _read(self):
        try:
            for line in iter(self.process.stdout.readline, b""):
                event = json.loads(line)
                if event.get("format_version") != 1:
                    raise ValueError("unexpected driver history version")
                self._events.write(line.decode("utf-8"))
                with self._pending_lock:
                    future = self._pending.get(event["correlation"])
                    if future is None:
                        raise ValueError("driver emitted an unsubmitted or repeated correlation")
                    if event["event"] != "call":
                        if event["event"] not in ("return", "error", "clock", "skipped"):
                            raise ValueError("unknown driver event")
                        self._pending.pop(event["correlation"])
                        future.set_result(event)
            if not self._closing:
                raise RuntimeError("driver ended before shutdown")
        except Exception as error:
            self._broken = str(error)
        finally:
            with self._pending_lock:
                for future in self._pending.values():
                    future.set_exception(RuntimeError(self._broken or "driver stopped with requests in flight"))
                self._pending.clear()

    def send(self, kind, endpoint, client_id, request_id, sql, metadata, *, timeout_ms=6000,
             deadline=None, cutoff_ns=0):
        deadline = min(deadline or self.control.deadline, self.control.deadline)
        while not self._slots.acquire(timeout=0.1):
            self.control.check(deadline)
        future = None
        try:
            self.control.check(deadline)
            with self._send_lock:
                if self._broken or self.process.poll() is not None:
                    raise RuntimeError(self._broken or "driver is not running")
                if metadata.get("phase") == "history":
                    if self._history_attempts >= 128:
                        raise EnvironmentLimit("history reached the 128-attempt limit; no hidden truncation")
                    self._history_attempts += 1
                self._next += 1
                correlation = self._next
                future = Future()
                # A caller timeout does not cancel Send in the bridge. Hold the
                # actual in-flight slot until its terminal event or bridge exit.
                future.add_done_callback(lambda _: self._slots.release())
                with self._pending_lock:
                    self._pending[correlation] = future
                record = {"format_version": 1, "correlation": correlation, "kind": kind,
                          "client_id": client_id, "request_id": request_id, "attempt": 1,
                          "logical_operation_id": (f"write:{client_id}:{request_id}" if kind == "WRITE"
                                                   else f"{kind.lower()}:{correlation}"),
                          "endpoint": endpoint, "sql": sql, "timeout_ms": timeout_ms,
                          "cutoff_ns": cutoff_ns, **metadata}
                self._inputs.write(json.dumps(record, sort_keys=True) + "\n")
                fields = ["T0A1", correlation, kind, client_id, request_id, timeout_ms, cutoff_ns,
                          endpoint or "-", sql.encode("utf-8").hex() or "-"]
                data = ("\t".join(map(str, fields)) + "\n").encode("ascii")
                offset = 0
                while offset < len(data):
                    self.control.check(deadline)
                    _, writable, _ = select.select([], [self.process.stdin], [], 0.1)
                    if writable:
                        try:
                            offset += os.write(self.process.stdin.fileno(), data[offset:])
                        except BlockingIOError:
                            continue
            while True:
                self.control.check(deadline)
                try:
                    return future.result(timeout=min(0.1, max(0.001, deadline - time.monotonic())))
                except FutureTimeout:
                    continue
        finally:
            if future is None:
                self._slots.release()

    def quiesce(self, deadline):
        while True:
            with self._pending_lock:
                if not self._pending:
                    return
            self.control.check(deadline)
            time.sleep(0.01)

    def clock(self):
        return self.send("CLOCK", "", 0, 0, "", {"phase": "clock"}, timeout_ms=0)["clock_ns"]

    def close(self, abort=False):
        self._closing = True
        if abort and self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.stdin.close()
            self.process.wait(timeout=10)
        except (subprocess.TimeoutExpired, BrokenPipeError):
            if self.process.poll() is None:
                self.process.kill()
            self.process.wait(timeout=5)
        self._reader.join(timeout=5)
        self.process.stdout.close()
        self._inputs.close()
        self._events.close()
        self._stderr.close()
        return {"exit_code": self.process.returncode, "history_error": self._broken,
                "reader_stopped": not self._reader.is_alive()}


class Cluster:
    def __init__(self, build, root, config):
        self.root = root
        self.port_base = config["port_base"]
        self._state_lock = threading.Lock()
        self.running = {}
        self.proxy_pids = []
        self.observers = []
        self._log = (root / "cluster.stderr").open("xb")
        self._timeline = (root / "faults.jsonl").open("x", buffering=1)
        script = Path(__file__).with_name("cluster.sh")
        command = ["bash", str(script), str(build), str(self.port_base), str(root / "nodes"),
                   str(config["buffer_pages"]), str(config["snapshot_threshold"]),
                   "1" if config.get("proxies") else "0"]
        try:
            self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                            stderr=self._log, start_new_session=True, bufsize=0)
        except Exception:
            self._log.close()
            self._timeline.close()
            raise
        try:
            fields = self._reply(time.monotonic() + 30)
            if len(fields) != 7 or fields[0] != "READY":
                raise RuntimeError("cluster did not report owned processes")
            self.running = {node: int(fields[node]) for node in (1, 2, 3)}
            self.proxy_pids = [int(pid) for pid in fields[4:] if int(pid)]
            self.event("started", nodes=self.running, proxies=self.proxy_pids,
                       routes={node: self.port_base + 200 + node for node in (1, 2, 3)}
                       if self.proxy_pids else {})
        except Exception:
            self.close()
            raise

    def event(self, event, **fields):
        self._timeline.write(json.dumps({"event": event, "monotonic_ns": time.monotonic_ns(), **fields}) + "\n")

    def _reply(self, deadline):
        while time.monotonic() < deadline:
            readable, _, _ = select.select([self.process.stdout], [], [], 0.1)
            if readable:
                line = self.process.stdout.readline()
                if not line:
                    raise RuntimeError("cluster control ended; see cluster.stderr")
                return line.decode().strip().split("\t")
            if self.process.poll() is not None:
                raise RuntimeError("cluster control failed; see cluster.stderr")
        raise TimeoutError("cluster control acknowledgement deadline exceeded")

    def endpoint(self, node):
        return f"127.0.0.1:{self.port_base + 100 + node}"

    def storage_usage(self):
        # Current backend adapter: database data only, excluding test histories
        # and proxy files. A raw-device backend must supply actual used-space
        # observations, never the size of its preallocated device.
        by_node = {str(node): allocated_bytes(self.root / "nodes" / f"node-{node}") for node in (1, 2, 3)}
        return {"kind": "filesystem_allocated_blocks", "by_node": by_node, "total_bytes": sum(by_node.values())}

    def live_endpoints(self, excluded=()):
        with self._state_lock:
            return [self.endpoint(node) for node in sorted(self.running) if node not in excluded]

    def samples(self):
        # Intentional stops and PID replacement are atomic with sampling. Never
        # treat a genuinely unexpected disappearance as an intentional fault.
        with self._state_lock:
            return [proc_sample(pid) for pid in self.running.values()]

    def add_observer(self, process):
        with self._state_lock:
            self.observers.append(process)

    def remove_observer(self, process):
        with self._state_lock:
            self.observers.remove(process)

    def observer_samples(self):
        with self._state_lock:
            samples = [proc_sample(pid) for pid in self.proxy_pids]
            for process in self.observers:
                if process.poll() is not None:
                    continue
                try:
                    samples.append(proc_sample(process.pid))
                except FileNotFoundError:
                    if process.poll() is None:
                        raise
            return samples

    def kill(self, node, deadline):
        with self._state_lock:
            pid = self.running[node]
            self.event("kill_issued", node=node, pid=pid, signal="SIGKILL")
            self.process.stdin.write(f"KILL {node}\n".encode())
            fields = self._reply(deadline)
            if fields != ["EXIT", str(node), str(pid), "137"]:
                raise RuntimeError(f"invalid kill/wait evidence: {fields}")
            del self.running[node]
            self.event("exit_waited", node=node, pid=pid, wait_status=137)

    def start(self, node, deadline):
        with self._state_lock:
            if node in self.running:
                raise RuntimeError("cannot restart a running node")
            self.event("restart_issued", node=node)
            self.process.stdin.write(f"START {node}\n".encode())
            fields = self._reply(deadline)
            if len(fields) != 3 or fields[:2] != ["STARTED", str(node)]:
                raise RuntimeError(f"invalid restart acknowledgement: {fields}")
            self.running[node] = int(fields[2])
            self.event("restart_acknowledged", node=node, pid=self.running[node])

    def close(self):
        if self.process.poll() is None:
            try:
                self.process.stdin.write(b"STOP\n")
                self.process.stdin.close()
            except BrokenPipeError:
                pass
        try:
            self.process.wait(timeout=35)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=5)
        self.process.stdout.close()
        self._log.close()
        self._timeline.close()
        return self.process.returncode


def proc_sample(pid):
    root = Path("/proc") / str(pid)
    values = (root / "stat").read_text().rsplit(")", 1)[1].split()
    io = {}
    io_error = None
    try:
        io = {key.rstrip(":"): int(value) for key, value in
              (line.split() for line in (root / "io").read_text().splitlines())}
    except OSError as error:
        io_error = str(error)
    return {"pid": pid, "cpu_ticks": int(values[11]) + int(values[12]),
            "start_ticks": int(values[19]), "rss_bytes": int(values[21]) * os.sysconf("SC_PAGE_SIZE"),
            "io": io, "io_error": io_error}


def memory_sample():
    return {line.split(":")[0]: int(line.split()[1]) * 1024
            for line in Path("/proc/meminfo").read_text().splitlines()
            if line.startswith(("MemTotal:", "MemAvailable:", "Cached:", "Buffers:", "Dirty:", "Writeback:"))}


def allocated_bytes(root):
    total = 0
    for parent, _, names in os.walk(root, followlinks=False):
        for name in names:
            try:
                total += (Path(parent) / name).lstat().st_blocks * 512
            except FileNotFoundError:
                # Snapshot/log replacement can remove a file during traversal.
                continue
    return total


class Monitor:
    def __init__(self, run_root, artifact_root, cluster, driver_pid, control):
        self.root = run_root
        self.artifact_root = artifact_root
        self.cluster = cluster
        self.clients = [os.getpid(), driver_pid]
        self.control = control
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="resource-sampler", daemon=True)
        self._thread.start()

    def _run(self):
        try:
            with (self.root / "resources.jsonl").open("x", encoding="utf-8", buffering=1) as output:
                while not self._stop.is_set():
                    started = time.monotonic_ns()
                    nodes = self.cluster.samples()
                    clients = [proc_sample(pid) for pid in self.clients] + self.cluster.observer_samples()
                    memory = memory_sample()
                    used = allocated_bytes(self.artifact_root)
                    free = shutil.disk_usage(self.artifact_root).free
                    sample = {"format_version": 1, "monotonic_ns": started,
                              "nodes": nodes, "clients": clients, "host_memory": memory,
                              "artifact_allocated_bytes": used, "filesystem_free_bytes": free,
                              "storage_usage": self.cluster.storage_usage(),
                              "host_diskstats": Path("/proc/diskstats").read_text(),
                              "collection_ns": time.monotonic_ns() - started}
                    output.write(json.dumps(sample, sort_keys=True) + "\n")
                    if sum(item["rss_bytes"] for item in nodes) > 3 * GIB:
                        self.control.fail("three-node RSS exceeded 3 GiB observation budget")
                    if sum(item["rss_bytes"] for item in clients) > GIB // 2:
                        self.control.fail("runner/driver RSS exceeded 512 MiB observation budget")
                    if memory["MemAvailable"] < GIB:
                        self.control.fail("host MemAvailable fell below 1 GiB")
                    if used > 10 * GIB or free < GIB:
                        self.control.fail("artifact budget exceeded or filesystem headroom below 1 GiB")
                    self._stop.wait(max(0, 1 - (time.monotonic_ns() - started) / 1e9))
        except Exception as error:
            self.control.fail(f"resource observation unavailable: {error}")

    def close(self):
        self._stop.set()
        self._thread.join(timeout=5)
        return not self._thread.is_alive()
