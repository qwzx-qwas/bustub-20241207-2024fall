"""External faults and version-specific evidence, shared by correctness and future P4."""

import json
from pathlib import Path
import subprocess
import sys
import time

from adapter import RequestFailure, retryable
from runtime import write_json


class NotCovered(RuntimeError):
    pass


class FaultIO:
    """Bounded delivery of the same intent; never fabricate service recovery."""

    def __init__(self, adapter, cluster, control):
        self.adapter, self.cluster, self.control = adapter, cluster, control
        self.excluded = set()

    def discover(self, deadline):
        return self.adapter.discover(deadline, self.cluster.live_endpoints(self.excluded))

    def resolve(self, intent, phase="history", replay=False):
        deadline = min(self.control.deadline, time.monotonic() + 60)
        while True:
            self.control.check(deadline)
            try:
                return self.adapter.attempt_write(intent, phase, deadline, replay=replay)
            except RequestFailure as error:
                if not retryable(error) or intent.attempts >= 8:
                    raise
                time.sleep(min(0.1 * 2**(intent.attempts - 1), 1))
                self.discover(deadline)

    def write(self, operation, client, phase="history"):
        return self.resolve(self.adapter.begin_write(operation, client), phase)

    def read(self, operation, phase="history"):
        deadline = min(self.control.deadline, time.monotonic() + 60)
        for attempt in range(1, 9):
            self.control.check(deadline)
            try:
                return self.adapter.read(operation, phase, deadline, attempt=attempt)
            except RequestFailure as error:
                if not retryable(error) or attempt == 8:
                    raise
                time.sleep(min(0.1 * 2**(attempt - 1), 1))
                self.discover(deadline)
        raise AssertionError("unreachable retry loop")

    def wait_status(self, node, predicate, deadline, observe=None):
        while True:
            self.control.check(deadline)
            try:
                status = self.adapter.status(self.cluster.endpoint(node), deadline)
                if observe is not None:
                    observe(status)
                if predicate(status):
                    return status
            except RequestFailure as error:
                if not retryable(error):
                    raise
            time.sleep(0.1)


class Network:
    def __init__(self, cluster, control):
        self.cluster, self.control = cluster, control
        self.generation = 0

    def controls(self, node):
        return self.cluster.root / "nodes" / f"proxy-{node}"

    def apply(self, isolated=None):
        self.generation += 1
        deadline = min(self.control.deadline, time.monotonic() + 60)
        expected = {}
        for node in (1, 2, 3):
            rule = {"generation": self.generation, "drop_all": node == isolated,
                    "blocked_from": [isolated] if isolated is not None and node != isolated else []}
            expected[node] = rule
            temporary = self.controls(node) / "rules.tmp"
            write_json(temporary, rule)
            temporary.replace(self.controls(node) / "rules.json")
        acknowledgements = {}
        while len(acknowledgements) != 3:
            self.control.check(deadline)
            for node in (1, 2, 3):
                path = self.controls(node) / "ack.json"
                if path.exists():
                    value = json.loads(path.read_text())
                    if value["generation"] == self.generation:
                        if any(value.get(key) != item for key, item in expected[node].items()):
                            raise RuntimeError("proxy acknowledged different isolation rules")
                        acknowledgements[node] = value
            time.sleep(0.01)
        self.cluster.event("network_rules_acknowledged", generation=self.generation,
                           isolated=isolated, acknowledgements=acknowledgements)
        return self.generation

    def events(self, since=0):
        events = []
        for node in (1, 2, 3):
            root = self.controls(node)
            if (root / "last-error").exists():
                raise RuntimeError((root / "last-error").read_text())
            path = root / "events.jsonl"
            if path.exists():
                # A concurrent writer can leave only the final line incomplete.
                # Re-read on the next bounded poll; never accept a partial record.
                data = path.read_bytes()
                for line in data.splitlines(keepends=True):
                    if not line.endswith(b"\n"):
                        continue
                    event = json.loads(line)
                    if event["monotonic_ns"] >= since:
                        events.append(event)
        return sorted(events, key=lambda event: event["monotonic_ns"])

    def blocked_both_directions(self, isolated, generation, deadline):
        while True:
            self.control.check(deadline)
            blocked = [event for event in self.events() if event["event"] == "blocked" and
                       event["generation"] == generation]
            outgoing = [event for event in blocked if event["from"] == isolated]
            incoming = [event for event in blocked if event["to"] == isolated]
            if incoming and outgoing:
                evidence = {"incoming": incoming[0], "outgoing": outgoing[0]}
                self.cluster.event("bidirectional_block_observed", **evidence)
                return evidence
            time.sleep(0.1)

    def catchup(self, node, mode, since, lower_bound, deadline):
        while True:
            self.control.check(deadline)
            events = [event for event in self.events(since) if event["event"] == "forwarded"]
            if mode == "short" and any(event["type"] == 5 and event["to"] == node for event in events):
                raise NotCovered("short catch-up used InstallSnapshot")
            # Snapshot chunks can be retransmitted with the same request ID.
            # A later stale acknowledgement must not overwrite proof that the
            # earlier request actually installed the snapshot.
            responses = {(event["type"], event["from"], event["to"], event["term"], event["request_id"]): event
                         for event in events if event["type"] in (4, 6) and event["success"] and
                         (event["type"] != 6 or (event["complete"] and not event["stale"]))}
            for request in events:
                kind = request["type"]
                if kind not in ((3, 5) if mode == "either" else (3,) if mode == "short" else (5,)) or request["to"] != node:
                    continue
                if kind == 3 and request.get("entry_count", 0) == 0:
                    continue
                if kind == 5 and (not request["done"] or request["last_included_index"] <= lower_bound):
                    continue
                response = responses.get((kind + 1, node, request["from"], request["term"], request["request_id"]))
                if response:
                    if kind == 3:
                        matched = response["match_index"] >= request["prev_log_index"] + request["entry_count"]
                    else:
                        # next_offset is a continuation cursor for partial
                        # transfers. Production completion replies clear it;
                        # completion and match_index acknowledge installation.
                        matched = (response["complete"] and not response["stale"] and
                                   response["match_index"] >= request["last_included_index"])
                    if matched:
                        return {"request": request, "response": response}
            time.sleep(0.1)


class DropResponse:
    def __init__(self, cluster, intent, target):
        self.path = cluster.root / "dropped-response.txt"
        self._log = (cluster.root / "drop-proxy.stderr").open("xb")
        script = Path(__file__).resolve().parents[1] / "support/raft_drop_response_proxy.py"
        self.endpoint = f"127.0.0.1:{cluster.port_base + 301}"
        try:
            self.process = subprocess.Popen([sys.executable, str(script), "--listen-port", str(cluster.port_base + 301),
                                             "--target-port", target.rsplit(":", 1)[1], "--result", str(self.path),
                                             "--request-id", str(intent.request_id)], stderr=self._log)
        except Exception:
            self._log.close()
            raise
        self.cluster = cluster
        cluster.add_observer(self.process)

    def ready(self, control, deadline):
        while not self.path.with_suffix(".txt.ready").exists():
            control.check(deadline)
            if self.process.poll() is not None:
                raise RuntimeError("response-drop proxy failed before ready")
            time.sleep(0.01)

    def evidence(self, deadline):
        status = self.process.wait(timeout=max(0.01, deadline - time.monotonic()))
        if status != 0:
            raise RuntimeError("response-drop proxy failed; no proven dropped commit")
        label, separator, payload = self.path.read_text().strip().partition("=")
        if label != "response_bytes" or not separator or len(bytes.fromhex(payload)) != 32:
            raise RuntimeError("invalid dropped-response evidence")
        return payload

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self._log.close()
        self.cluster.remove_observer(self.process)
