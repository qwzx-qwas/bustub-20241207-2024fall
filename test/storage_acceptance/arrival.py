"""Test-side, bounded fixed-arrival foreground traffic for P4.

YCSB's intended-start idea is used to retain schedule delay. Unlike a closed
loop, a slow server does not silently lower the requested arrival rate.
"""

from collections import deque
import json
import math
import threading
import time

from content import p4_operations
from delivery import LogicalFailure
from runtime import write_json


class ArrivalTraffic:
    def __init__(self, adapter, rows, root, config, control):
        self.adapter, self.rows, self.root, self.control = adapter, rows, root, control
        self.rate, self.seed = config["rate"], config["seed"]
        self.request_seconds = config.get("request_seconds", 60)
        self.condition = threading.Condition()
        self.queues = [deque(), deque(), deque()]
        self.active_writes = 0
        self.blocked_writers = {}
        self.unissued = []
        self.service_failures = []
        self.writes_paused = False
        self.confirmed_writes = 0
        self.last_index = 0
        self.failures = []
        self.stopping = threading.Event()
        self.generated = threading.Event()
        self.abort = False
        self.plan_count = self.rejected = 0
        self.max_queued = self.max_inflight = self.inflight = 0
        self.threads = []

    def start(self):
        before = time.monotonic_ns()
        self.start_ns = self.adapter.driver.clock()
        after = time.monotonic_ns()
        # Scheduling from receipt gives a conservative anchor: never dispatch
        # before the corresponding driver-clock plan. Record its IPC delay bound.
        self.python_start_ns = after
        self.clock_uncertainty_ns = after - before
        write_json(self.root / "arrival_clock.json", {"start_ns": self.start_ns,
                   "python_start_ns": self.python_start_ns, "translation_uncertainty_ns": self.clock_uncertainty_ns})
        self.threads = [threading.Thread(target=self._worker, args=(0, worker), daemon=True,
                                        name=f"arrival-read-{worker}") for worker in range(4)]
        self.threads += [threading.Thread(target=self._worker, args=(writer + 1, writer), daemon=True,
                                         name=f"arrival-write-{writer}") for writer in range(2)]
        self.threads.append(threading.Thread(target=self._generate, daemon=True, name="arrival-scheduler"))
        for thread in self.threads:
            thread.start()

    def _failed(self, error):
        with self.condition:
            self.failures.append(error)
            self.abort = True
            self.stopping.set()
            self.condition.notify_all()

    def check(self, deadline=None):
        self.control.check(deadline)
        with self.condition:
            if self.failures:
                raise self.failures[0]

    def _generate(self):
        try:
            with (self.root / "arrivals.jsonl").open("x", buffering=1) as output:
                for plan_id, writer, operation in p4_operations(len(self.rows), self.seed):
                    offset = int((plan_id - 1) * 1e9 / self.rate)
                    target = self.python_start_ns + offset
                    while not self.stopping.is_set() and time.monotonic_ns() < target:
                        self.control.check()
                        self.stopping.wait(min(0.1, max(0, (target - time.monotonic_ns()) / 1e9)))
                    if self.stopping.is_set():
                        break
                    self.control.check()
                    slot = 0 if writer is None else writer + 1
                    item = {"plan_id": plan_id, "planned_ns": self.start_ns + offset,
                            "operation": operation, "writer": writer}
                    with self.condition:
                        if self.stopping.is_set():
                            break
                        queued = sum(len(queue) for queue in self.queues)
                        admitted = queued < 64 and not self.abort and slot not in self.blocked_writers
                        reason = None if admitted else ("writer_unresolved" if slot in self.blocked_writers else "queue_full_or_aborted")
                        if admitted:
                            self.queues[slot].append(item)
                            self.max_queued = max(self.max_queued, queued + 1)
                        else:
                            self.rejected += 1
                        self.plan_count += 1
                        self.condition.notify_all()
                    output.write(json.dumps({"plan_id": plan_id, "planned_ns": item["planned_ns"],
                                             "generated_python_ns": time.monotonic_ns(),
                                             "kind": "READ" if writer is None else "WRITE",
                                             "admitted": admitted,
                                             "reason": reason}) + "\n")
        except Exception as error:
            self._failed(error)
        finally:
            self.generated.set()
            with self.condition:
                self.condition.notify_all()

    def _worker(self, slot, worker):
        try:
            while True:
                with self.condition:
                    while not self.abort and (not self.queues[slot] or (slot and self.writes_paused)):
                        if self.generated.is_set() and not self.queues[slot]:
                            return
                        self.condition.wait(timeout=0.1)
                        self.control.check()
                    if self.abort:
                        return
                    item = self.queues[slot].popleft()
                    self.inflight += 1
                    self.max_inflight = max(self.max_inflight, self.inflight)
                    self.active_writes += bool(slot)
                try:
                    operation = item["operation"]
                    trace = {"plan_id": item["plan_id"], "planned_ns": item["planned_ns"],
                             "planned_python_ns": self.python_start_ns + item["planned_ns"] - self.start_ns}
                    if slot:
                        response = self.adapter.write(operation, 100 + worker, "foreground",
                                                      self.control.deadline, trace=trace)
                        with self.condition:
                            self.confirmed_writes += 1
                            self.last_index = max(self.last_index, response["committed_index"])
                    else:
                        actual = self.adapter.read(operation, "foreground", self.control.deadline, trace=trace)
                        original = self.rows[operation.args["id"]].values()
                        # Mutable columns are checked after drain against all confirmed
                        # inputs. P4 does not pretend this is a linearizability proof.
                        if (len(actual) != 1 or actual[0][:3] != original[:3] or
                                actual[0][5] != original[5]):
                            raise AssertionError("foreground read lost/mixed immutable business fields")
                except LogicalFailure as error:
                    with self.condition:
                        self.service_failures.append({"plan_id": item["plan_id"], "outcome": error.outcome})
                        if slot and error.outcome != "NOT_ISSUED":
                            self.blocked_writers[slot] = time.monotonic_ns()
                            while self.queues[slot]:
                                queued = self.queues[slot].popleft()
                                self.unissued.append({"plan_id": queued["plan_id"], "reason": "writer_unresolved"})
                finally:
                    with self.condition:
                        self.inflight -= 1
                        self.active_writes -= bool(slot)
                        self.condition.notify_all()
        except Exception as error:
            self._failed(error)

    def wait_until(self, deadline):
        while time.monotonic() < deadline:
            self.check()
            self.stopping.wait(min(0.05, max(0, deadline - time.monotonic())))

    def progress(self):
        with self.condition:
            return self.confirmed_writes, self.last_index

    def pause_writes(self, deadline):
        with self.condition:
            self.writes_paused = True
            while self.active_writes:
                self.condition.wait(timeout=0.05)
                self.control.check(deadline)
                if self.failures:
                    raise self.failures[0]
            return self.confirmed_writes, self.last_index

    def resume_writes(self):
        with self.condition:
            self.writes_paused = False
            self.condition.notify_all()

    def close(self, abort=False):
        self.end_python_ns = time.monotonic_ns()
        self.end_ns = self.start_ns + self.end_python_ns - self.python_start_ns
        self.stopping.set()
        with self.condition:
            self.abort |= abort
            for queue in self.queues:
                while queue:
                    self.unissued.append({"plan_id": queue.popleft()["plan_id"], "reason": "window_ended"})
            self.writes_paused = False
            self.condition.notify_all()
        deadline = min(self.control.deadline, time.monotonic() + self.request_seconds + 1)
        try:
            for thread in self.threads:
                if thread.ident is None:
                    continue
                while thread.is_alive():
                    self.control.check(deadline)
                    thread.join(timeout=0.05)
        finally:
            with self.condition:
                self.abort = True
                self.condition.notify_all()
            for thread in self.threads:
                if thread.ident is not None:
                    thread.join(timeout=0.2)
            stopped = all(not thread.is_alive() for thread in self.threads)
            if not stopped:
                self.control.fail("foreground worker did not stop; history may be incomplete")
            expected = max(0, math.ceil((self.end_ns - self.start_ns) * self.rate / 1e9))
            write_json(self.root / "arrival_dispatch.json", {
                "start_ns": self.start_ns, "end_ns": self.end_ns,
                "expected_arrivals": expected, "generated_arrivals": self.plan_count,
                "generator_unproduced": max(0, expected - self.plan_count), "queue_rejections": self.rejected,
                "queued_unissued": sum(len(queue) for queue in self.queues),
                "unissued_plans": self.unissued, "service_failures": self.service_failures,
                "blocked_writers": self.blocked_writers,
                "max_queued": self.max_queued, "max_inflight": self.max_inflight,
                "confirmed_writes": self.confirmed_writes, "last_index": self.last_index,
                "all_workers_stopped": stopped, "errors": [str(error) for error in self.failures]})
