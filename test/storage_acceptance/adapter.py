"""Current-version SQL/response adaptation, entirely on the test side."""

from dataclasses import dataclass
import threading
import time

from content import COLUMNS


ADAPTER_VERSION = "bustub-client-v1-c1"
SCHEMA_SQL = ("CREATE TABLE bench_state(id INT PRIMARY KEY, tenant_id INT, kind INT, "
              "version INT, last_op BIGINT, payload VARCHAR(1024));")


def quote(value):
    return "'" + value.replace("'", "''") + "'"


def to_sql(operation):
    name, args = operation.name, operation.args
    columns = ", ".join(COLUMNS)
    if name == "CreateSchema":
        return SCHEMA_SQL
    if name == "InsertRows":
        if not args["rows"]:
            raise ValueError("empty insert is not effective work")
        values = []
        for row in args["rows"]:
            values.append("(" + ", ".join(quote(row[column]) if column == "payload" else str(row[column])
                                           for column in COLUMNS) + ")")
        return f"INSERT INTO bench_state VALUES {', '.join(values)};"
    if name == "ReadPoint":
        return f"SELECT {columns} FROM bench_state WHERE id = {args['id']};"
    if name == "ReadRange":
        # The negative sentinel means the entire lower INT domain. Avoid asking
        # the parser to construct positive 2147483648 before applying unary '-'.
        lower = "" if args["low"] == -(2**31) else f"id >= {args['low']} AND "
        return f"SELECT {columns} FROM bench_state WHERE {lower}id < {args['high']} ORDER BY id;"
    if name == "Delete":
        return f"DELETE FROM bench_state WHERE id = {args['id']};"
    if name in ("Bump", "ReplacePayload"):
        payload = f", payload = {quote(args['payload'])}" if name == "ReplacePayload" else ""
        return (f"UPDATE bench_state SET version = version + 1, last_op = {args['last_op']}"
                f"{payload} WHERE id = {args['id']};")
    raise ValueError(f"unsupported business operation: {name}")


class RequestFailure(RuntimeError):
    def __init__(self, outcome, response):
        self.outcome = outcome
        self.response = response
        super().__init__(f"{outcome}: {response}")


def require_success(response, write):
    if response["event"] == "skipped":
        raise RequestFailure("NOT_ISSUED", response)
    if response["event"] == "error":
        # Send can throw for transport OR response validation. Neither authorizes a fresh write identity.
        raise RequestFailure("UNKNOWN", response)
    status = response.get("status")
    if status in ("TIMEOUT", "UNAVAILABLE"):
        raise RequestFailure("UNKNOWN", response)
    if status in ("REJECTED", "NOT_LEADER"):
        raise RequestFailure(status, response)
    if response["event"] != "return" or status != ("COMMITTED" if write else "OK"):
        raise RequestFailure("INVALID_RESPONSE", response)
    if write and (response.get("committed_request_id") != response["request_id"] or
                  response.get("committed_index", 0) > response["published_applied_index"]):
        raise RequestFailure("INVALID_RESPONSE", response)
    return response


@dataclass
class WriteIntent:
    operation: object
    client_id: int
    request_id: int
    sql: str
    attempts: int = 0
    result: object = None


def retryable(error):
    """Transport ambiguity is distinct from malformed/uncorrelated responses."""
    if not isinstance(error, RequestFailure):
        return False
    if error.response.get("event") == "error":
        message = error.response.get("error", "")
        return message.startswith("cannot connect to client endpoint ") or message in (
            "distributed client request write failed",
            "distributed client response prefix is unavailable",
            "distributed client response is truncated")
    return error.response.get("status") in ("NOT_LEADER", "TIMEOUT", "UNAVAILABLE")


class Adapter:
    def __init__(self, driver, endpoints, timeout_ms=6000):
        self.driver = driver
        self.endpoints = endpoints
        self.timeout_ms = timeout_ms
        self.leader = None
        self._write_ids = {}
        self._pending = {}
        self._last = {}
        self._inflight = set()
        self._read_id = 0
        self._lock = threading.Lock()

    def _read_number(self):
        with self._lock:
            self._read_id += 1
            return self._read_id

    def status(self, endpoint, deadline):
        result = self.driver.send("STATUS", endpoint, 0, self._read_number(), "",
                                  {"phase": "status", "operation": "Status"},
                                  timeout_ms=min(self.timeout_ms, 1000), deadline=deadline)
        require_success(result, write=False)
        return result

    def discover(self, deadline, endpoints=None):
        while time.monotonic() < deadline:
            for endpoint in self.endpoints if endpoints is None else endpoints:
                if time.monotonic() >= deadline:
                    break
                try:
                    result = self.status(endpoint, deadline)
                except RequestFailure as error:
                    if not retryable(error):
                        raise
                    continue
                if result.get("leader_ready") and result.get("leader_id") == result.get("node_id"):
                    self.leader = endpoint
                    return result
            time.sleep(0.1)
        raise TimeoutError("no ready Leader before absolute readiness deadline")

    def begin_write(self, operation, client_id):
        with self._lock:
            if client_id in self._pending or client_id in self._inflight:
                raise RuntimeError("cannot advance a client with an unresolved write")
            intent = WriteIntent(operation, client_id, self._write_ids.get(client_id, 1), to_sql(operation))
            self._pending[client_id] = intent
            return intent

    def discard_unissued(self, intent):
        with self._lock:
            if intent.attempts or intent.client_id in self._inflight:
                raise RuntimeError("cannot discard an issued/possibly committed write")
            if self._pending.get(intent.client_id) is not intent:
                raise RuntimeError("unissued identity is not pending")
            del self._pending[intent.client_id]

    def attempt_write(self, intent, phase, deadline, endpoint=None, cutoff_ns=0, replay=False, trace=None,
                      max_attempts=8):
        with self._lock:
            expected = self._last.get(intent.client_id) if replay else self._pending.get(intent.client_id)
            if expected is not intent or intent.client_id in self._inflight:
                raise RuntimeError("write identity is not the retained pending/last request")
            if max_attempts is not None and intent.attempts >= max_attempts:
                raise TimeoutError("logical write exhausted eight network attempts")
            self._inflight.add(intent.client_id)
            intent.attempts += 1
        try:
            result = self.driver.send("WRITE", endpoint or self.leader, intent.client_id, intent.request_id,
                                      intent.sql, {**(trace or {}), "phase": phase, "operation": intent.operation.record(),
                                                   "attempt": intent.attempts},
                                      timeout_ms=self.timeout_ms, deadline=deadline, cutoff_ns=cutoff_ns)
            require_success(result, write=True)
            with self._lock:
                if replay:
                    if result["payload_hex"] != intent.result["payload_hex"]:
                        raise AssertionError("retry changed the committed business response")
                else:
                    intent.result = result
                    self._last[intent.client_id] = intent
                    del self._pending[intent.client_id]
                    self._write_ids[intent.client_id] = intent.request_id + 1
            return result
        except RequestFailure as error:
            if error.outcome == "NOT_ISSUED":
                with self._lock:
                    self._pending.pop(intent.client_id, None)
            raise
        finally:
            with self._lock:
                self._inflight.remove(intent.client_id)

    def write(self, operation, client_id, phase, deadline, cutoff_ns=0, trace=None):
        # Healthy scenarios deliberately make only one attempt. An unsuccessful
        # write retains its identity and prevents silently advancing the session.
        return self.attempt_write(self.begin_write(operation, client_id), phase, deadline,
                                  cutoff_ns=cutoff_ns, trace=trace)

    def last_write(self, client_id):
        with self._lock:
            return self._last[client_id]

    def read(self, operation, phase, deadline, *, endpoint=None, stale=False, attempt=1, cutoff_ns=0, trace=None):
        result = self.driver.send("STALE" if stale else "READ", endpoint or self.leader, 0,
                                  self._read_number(), to_sql(operation),
                                  {**(trace or {}), "phase": phase, "operation": operation.record(), "attempt": attempt},
                                  timeout_ms=self.timeout_ms, deadline=deadline, cutoff_ns=cutoff_ns)
        require_success(result, write=False)
        rows = result.get("rows")
        if not isinstance(rows, list) or len(result.get("columns", [])) != len(COLUMNS):
            raise RequestFailure("INVALID_RESPONSE", result)
        if any(len(row) != len(COLUMNS) or any(not isinstance(value, str) for value in row) for row in rows):
            raise RequestFailure("INVALID_RESPONSE", result)
        return rows
