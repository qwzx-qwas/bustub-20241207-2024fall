"""Performance-only delivery policy; production APIs and correctness policy stay separate.

Each logical operation retains all attempts, one absolute observation deadline,
and (for writes) one identity. A deadline is not evidence of rollback/cancellation.
"""

from contextlib import contextmanager
import json
import threading
import time

from adapter import RequestFailure, retryable
from runtime import write_json


class LogicalFailure(RequestFailure):
    """An observed service failure, as distinct from a broken measuring tool."""


class PerformanceIO:
    def __init__(self, adapter, root, config, control):
        self.base, self.driver, self.root, self.control = adapter, adapter.driver, root, control
        self.request_seconds = config["request_seconds"]
        self.stage_deadline = control.deadline
        self.stage_name = "startup"
        self.pending = {}
        self._lock = threading.Lock()
        self._number = 0
        self.events = (root / "logical_history.jsonl").open("x", buffering=1)
        self.stages = (root / "stages.jsonl").open("x", buffering=1)
        self.clock_ns = self.driver.clock()
        self.python_ns = time.monotonic_ns()

    def __getattr__(self, name):
        return getattr(self.base, name)

    def stamp(self, driver_ns):
        return self.python_ns + driver_ns - self.clock_ns

    def emit(self, record):
        with self._lock:
            self.events.write(json.dumps(record, sort_keys=True) + "\n")

    @contextmanager
    def stage(self, name, seconds):
        previous = self.stage_deadline, self.stage_name
        self.stage_name = name
        self.stage_deadline = min(self.control.deadline, time.monotonic() + seconds)
        started = time.monotonic_ns()
        self.stages.write(json.dumps({"event": "start", "stage": name, "started_python_ns": started,
                                     "deadline_python_ns": int(self.stage_deadline * 1e9)}) + "\n")
        write_json(self.root / "current_stage.json", {"stage": name, "started_python_ns": started})
        outcome = "completed"
        try:
            yield self.stage_deadline
        except BaseException:
            outcome = "interrupted"
            raise
        finally:
            self.stages.write(json.dumps({"event": "end", "stage": name, "outcome": outcome,
                                         "ended_python_ns": time.monotonic_ns()}) + "\n")
            self.stage_deadline, self.stage_name = previous

    def _deliver(self, operation, phase, deadline, action, *, intent=None, trace=None, cutoff_ns=0,
                 resolving=False):
        trace = trace or {}
        now = time.monotonic_ns()
        start = trace.get("planned_python_ns", now)
        # Later reconciliation is a separate observation and never rewrites an
        # earlier deadline outcome or credits a completion to the old window.
        deadline = min(deadline, self.stage_deadline, self.control.deadline,
                       start / 1e9 + self.request_seconds)
        with self._lock:
            self._number += 1
            logical_id = f"logical:{self._number}"
        record = {"logical_id": logical_id, "operation": operation.record(), "phase": phase,
                  "stage": self.stage_name, "kind": "WRITE" if intent else "READ",
                  "started_python_ns": start, "dispatch_python_ns": now,
                  "deadline_python_ns": int(deadline * 1e9), "resolution": resolving, **trace}
        if intent:
            record.update(client_id=intent.client_id, request_id=intent.request_id)
        self.emit({**record, "event": "start"})
        attempts = 0
        last_error = None
        outcome = "INCOMPLETE"
        response = None
        try:
            if cutoff_ns and now >= self.stamp(cutoff_ns):
                outcome = "NOT_ISSUED"
                raise LogicalFailure(outcome, {"reason": "measurement window ended before dispatch"})
            if now >= int(deadline * 1e9):
                outcome = "NOT_ISSUED"
                raise LogicalFailure(outcome, {"reason": "queued operation exhausted its observation budget"})
            while True:
                self.control.check(deadline)
                attempts += 1
                try:
                    response = action(attempts, deadline, {**trace, "logical_id": logical_id})
                    outcome = "SUCCESS"
                    return response
                except RequestFailure as error:
                    last_error = str(error)
                    if error.outcome == "NOT_ISSUED":
                        outcome = "NOT_ISSUED"
                        raise LogicalFailure(outcome, error.response) from error
                    if not retryable(error):
                        if error.outcome == "REJECTED":
                            outcome = "FAILED"
                            raise LogicalFailure(outcome, error.response) from error
                        outcome = "INVALID_RESPONSE"
                        raise
                    # The total timer includes all rediscovery and backoff.
                    time.sleep(min(0.1 * 2 ** min(attempts - 1, 4), 1,
                                   max(0, deadline - time.monotonic())))
                    self.base.discover(deadline)
        except TimeoutError as error:
            outcome = "UNCONFIRMED"
            raise LogicalFailure(outcome, {"reason": str(error), "last_error": last_error,
                                          "deadline_python_ns": int(deadline * 1e9)}) from error
        finally:
            observed = time.monotonic_ns()
            until = min(observed, int(deadline * 1e9)) if outcome == "UNCONFIRMED" else observed
            self.emit({**record, "event": "end", "outcome": outcome, "attempts": attempts,
                       "observed_until_python_ns": until, "recorded_python_ns": observed,
                       "elapsed_ns": max(0, until - start), "censored": outcome == "UNCONFIRMED",
                       "last_error": last_error,
                       "committed_index": response.get("committed_index") if intent and response else None})

    def write(self, operation, client_id, phase, deadline, cutoff_ns=0, trace=None):
        if cutoff_ns and time.monotonic_ns() >= self.stamp(cutoff_ns):
            raise LogicalFailure("NOT_ISSUED", {"reason": "window closed"})
        intent = self.base.begin_write(operation, client_id)
        self.pending[client_id] = intent
        try:
            result = self._deliver(operation, phase, deadline,
                                   lambda attempt, end, tags: self.base.attempt_write(
                                       intent, phase, end, cutoff_ns=cutoff_ns if attempt == 1 else 0,
                                       trace=tags, max_attempts=None),
                                   intent=intent, trace=trace, cutoff_ns=cutoff_ns)
            del self.pending[client_id]
            return result
        except LogicalFailure as error:
            if error.outcome == "NOT_ISSUED" and intent.attempts == 0:
                # No network submission; retaining an empty intent would block
                # the session even though there is nothing to reconcile.
                self.base.discard_unissued(intent)
                self.pending.pop(client_id, None)
            elif error.outcome == "NOT_ISSUED":
                self.pending.pop(client_id, None)
            raise

    def read(self, operation, phase, deadline, *, endpoint=None, stale=False, cutoff_ns=0, trace=None):
        return self._deliver(operation, phase, deadline,
                             lambda attempt, end, tags: self.base.read(
                                 operation, phase, end, endpoint=endpoint, stale=stale, attempt=attempt,
                                 cutoff_ns=cutoff_ns if attempt == 1 else 0, trace=tags),
                             trace=trace, cutoff_ns=cutoff_ns)

    def reconcile(self, oracle, deadline, *, apply=True):
        # An expired Python waiter can leave an actual bridge call alive. Wait
        # for those calls before resubmitting retained write identities.
        self.driver.quiesce(deadline)
        last_index = 0
        for client, intent in list(self.pending.items()):
            response = self._deliver(intent.operation, "reconcile", deadline,
                          lambda attempt, end, tags: self.base.attempt_write(
                              intent, "reconcile", end, trace=tags, max_attempts=None),
                          intent=intent, resolving=True)
            last_index = max(last_index, response["committed_index"])
            if apply:
                oracle.apply(intent.operation)
            del self.pending[client]
        return last_index

    def close(self):
        self.events.close()
        self.stages.close()
