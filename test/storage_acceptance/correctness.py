"""Business correctness scenarios; no performance assertions."""

import threading
import time

from adapter import RequestFailure, retryable
from content import bump, c1_updates, c1_deletes, c1_reinserts, history_operations, insert_rows, read_point
from faults import DropResponse, FaultIO, Network, NotCovered
from runtime import write_json
from workload import prepare, verify_all


def c1(adapter, root, config, control):
    rows, oracle = prepare(adapter, root, config, control)
    stages = (("update", c1_updates(rows)), ("delete", c1_deletes(rows)))
    counts = {"grown_rows": 0, "shrunk_rows": 0}
    for phase, operations in stages:
        count = 0
        for operation in operations:
            if operation.name == "ReplacePayload":
                before = len(oracle.rows[operation.args["id"]].payload)
                after = len(operation.args["payload"])
                counts["grown_rows"] += after > before
                counts["shrunk_rows"] += after < before
            adapter.write(operation, 3, phase, control.deadline)
            oracle.apply(operation)
            count += 1
        if count == 0:
            raise AssertionError(f"C1 {phase} performed no effective work")
        counts[phase] = count
        for key in (0, 1, 2, 9, len(rows) - 1):
            operation = read_point(key)
            oracle.check(operation, adapter.read(operation, f"verify_{phase}", control.deadline))
        verify_all(adapter, oracle, len(rows), f"verify_{phase}", control.deadline)
    if not counts["grown_rows"] or not counts["shrunk_rows"]:
        raise AssertionError("C1 must exercise both payload growth and shrinkage")
    reinserts = list(c1_reinserts(rows))
    for offset in range(0, len(reinserts), 64):
        operation = insert_rows(reinserts[offset:offset + 64])
        adapter.write(operation, 3, "reinsert", control.deadline)
        oracle.apply(operation)
    counts["reinsert_rows"] = len(reinserts)
    verify_all(adapter, oracle, len(rows), "verify_final", control.deadline)
    for key in (1, 5, 9):
        operation = read_point(key)
        oracle.check(operation, adapter.read(operation, "verify_final", control.deadline))
    # This independent cardinality is fixed by C1, not derived from a response or the oracle.
    if len(rows) != 2048 or len(oracle.rows) != 1792:
        raise AssertionError("C1 fixed 2048 -> 1792 contract was changed")
    return {"status": "passed", "scenario": "C1", "effective_work": counts, "final_rows": 1792}


# C2-C5 use the same production adapter and fixture as C1. Fault support owns
# external actions, never a database recovery API or a private storage layout.
def start_history(root, oracle):
    write_json(root / "history_initial.json", oracle.rows[7].values())


def finish_history(adapter, root, control, fault_end_ns=None):
    end = adapter.driver.clock()
    boundary = {"end_ns": end}
    if fault_end_ns is not None:
        boundary["fault_end_ns"] = fault_end_ns
    write_json(root / "history_boundary.json", boundary)
    control.check()


def concurrent_history(io, root, config, fault=None):
    condition = threading.Condition()
    start = threading.Event()
    state = {"bumps": 0, "fault_complete": fault is None, "stop": False}
    failures = []

    def worker(index):
        start.wait()
        try:
            for operation in history_operations(config["seed"], index):
                with condition:
                    while state["bumps"] >= 16 and not state["fault_complete"] and not state["stop"]:
                        condition.wait(timeout=0.1)
                        io.control.check()
                    if state["stop"]:
                        return
                if operation.name == "Bump":
                    io.write(operation, 100 + index)
                    with condition:
                        state["bumps"] += 1
                        condition.notify_all()
                else:
                    io.read(operation)
        except Exception as error:
            with condition:
                failures.append(error)
                state["stop"] = True
                condition.notify_all()

    threads = [threading.Thread(target=worker, args=(index,), daemon=True, name=f"history-{index}")
               for index in range(4)]
    fault_boundary = None
    try:
        for thread in threads:
            thread.start()
        start.set()
        if fault is not None:
            with condition:
                while state["bumps"] < 16 and not failures:
                    condition.wait(timeout=0.1)
                    io.control.check()
                if failures:
                    raise failures[0]
            # No module lock is held while terminating processes or changing routes.
            fault()
            fault_boundary = io.adapter.driver.clock()
            with condition:
                state["fault_complete"] = True
                condition.notify_all()
        for thread in threads:
            while thread.is_alive():
                io.control.check()
                thread.join(timeout=0.1)
        if failures:
            raise failures[0]
        if state["bumps"] != 32:
            raise AssertionError("history did not complete the prescribed 32 logical writes")
        return fault_boundary
    finally:
        with condition:
            state["stop"] = True
            condition.notify_all()
        start.set()
        if any(thread.is_alive() for thread in threads):
            io.control.fail("history interrupted; retain unfinished attempts as unknown")
        for thread in threads:
            if thread.ident is not None:
                thread.join(timeout=2)
        write_json(root / "history_workers.json", {"completed_bumps": state["bumps"],
                                                   "errors": [str(error) for error in failures],
                                                   "all_workers_stopped": all(not t.is_alive() for t in threads)})


def c2(adapter, root, config, control, cluster):
    _, oracle = prepare(adapter, root, config, control)
    start_history(root, oracle)
    io = FaultIO(adapter, cluster, control)
    concurrent_history(io, root, config)
    io.read(read_point(7))
    finish_history(adapter, root, control)
    return {"status": "passed", "scenario": "C2", "requires_history": True,
            "requires_overlap": True, "logical_writes": 32, "logical_reads": 33}


def c3_drop(adapter, root, config, control, cluster):
    _, oracle = prepare(adapter, root, config, control)
    io = FaultIO(adapter, cluster, control)
    for number in range(10):
        operation = bump(7, number + 1)
        io.write(operation, 3, "drop_precondition")
        oracle.apply(operation)
    oracle.check(read_point(7), io.read(read_point(7), "verify_precondition"))
    start_history(root, oracle)
    operation = bump(7, 100001)
    intent = adapter.begin_write(operation, 100)
    proxy = DropResponse(cluster, intent, adapter.leader)
    deadline = min(control.deadline, time.monotonic() + 60)
    try:
        proxy.ready(control, deadline)
        try:
            adapter.attempt_write(intent, "history", deadline, endpoint=proxy.endpoint)
        except RequestFailure as error:
            if error.outcome != "UNKNOWN" or not retryable(error):
                raise
        else:
            raise AssertionError("drop-response request unexpectedly reached the client")
        lost_payload = proxy.evidence(deadline)
        cluster.event("committed_response_dropped", client_id=intent.client_id,
                      request_id=intent.request_id, payload_hex=lost_payload)
    finally:
        proxy.close()
    fault_end = adapter.driver.clock()
    result = io.resolve(intent)
    if result["payload_hex"] != lost_payload:
        raise AssertionError("retried business response differs from the dropped committed response")
    oracle.apply(operation)
    for _ in range(8):
        oracle.check(read_point(7), io.read(read_point(7)))
    if oracle.rows[7].version != 11:
        raise AssertionError("drop scenario changed its independent 10 -> 11 expectation")
    verify_all(adapter, oracle, config["rows"], "verify_final", control.deadline)
    finish_history(adapter, root, control, fault_end)
    return {"status": "passed", "scenario": "C3", "variant": "drop", "requires_history": True,
            "requires_post_fault": True, "expected_version": 11, "drop_payload_hex": lost_payload}


def c3(adapter, root, config, control, cluster):
    if config["variant"] == "drop":
        return c3_drop(adapter, root, config, control, cluster)
    _, oracle = prepare(adapter, root, config, control)
    start_history(root, oracle)
    io = FaultIO(adapter, cluster, control)
    network = Network(cluster, control)
    fault_state = {}

    def action():
        deadline = min(control.deadline, time.monotonic() + 60)
        leader = io.discover(deadline)["node_id"]
        fault_state["node"] = leader
        io.excluded.add(leader)
        if config["variant"] == "kill":
            cluster.kill(leader, deadline)
        else:
            generation = network.apply(leader)
            network.blocked_both_directions(leader, generation, deadline)
            # Brand-new identity after all rules ACK. Even if earlier requests
            # had committed, this one cannot be acknowledged without a quorum.
            intent = adapter.begin_write(bump(7, 200001), 900)
            fault_state["probe"] = intent
            try:
                adapter.attempt_write(intent, "history", deadline, endpoint=cluster.endpoint(leader))
            except RequestFailure as error:
                if not retryable(error):
                    raise
                cluster.event("isolated_leader_probe", outcome=error.outcome,
                              client_id=intent.client_id, request_id=intent.request_id)
            else:
                raise AssertionError("isolated old Leader committed a new write")
        io.discover(deadline)

    fault_end = concurrent_history(io, root, config, action)
    deadline = min(control.deadline, time.monotonic() + 60)
    if config["variant"] == "kill":
        cluster.start(fault_state["node"], deadline)
    else:
        network.apply()
    io.excluded.clear()
    io.discover(deadline)
    if "probe" in fault_state:
        io.resolve(fault_state["probe"])
    io.read(read_point(7))
    finish_history(adapter, root, control, fault_end)
    return {"status": "passed", "scenario": "C3", "variant": config["variant"],
            "requires_history": True, "requires_overlap": True, "requires_post_fault": True}


def sequential_bumps(io, oracle, count, start, phase, client=3):
    result = None
    for number in range(start, start + count):
        operation = bump(number % len(oracle.rows), 300000 + number)
        result = io.write(operation, client, phase)
        oracle.apply(operation)
    return result


def c4(adapter, root, config, control, cluster):
    _, oracle = prepare(adapter, root, config, control)
    io = FaultIO(adapter, cluster, control)
    status = io.discover(min(control.deadline, time.monotonic() + 60))
    prepared = 0
    while status["snapshot_base_index"] == 0 and prepared < 256:
        sequential_bumps(io, oracle, 16, prepared, "snapshot_preparation")
        prepared += 16
        status = io.discover(min(control.deadline, time.monotonic() + 60))
    if status["snapshot_base_index"] == 0:
        raise NotCovered("no nonzero recovery base within 256 preparation writes")
    sequential_bumps(io, oracle, 16, prepared, "confirmed_suffix")
    retained = adapter.last_write(3)
    committed = retained.result["committed_index"]
    write_json(root / "restart_boundary.json", {"last_confirmed_index": committed,
                                               "client_id": retained.client_id, "request_id": retained.request_id,
                                               "payload_hex": retained.result["payload_hex"],
                                               "observed_recovery_base": status["snapshot_base_index"]})
    deadline = min(control.deadline, time.monotonic() + 60)
    for node in (1, 2, 3):
        cluster.kill(node, deadline)
    for node in (1, 2, 3):
        cluster.start(node, deadline)
    io.discover(deadline)
    verify_all(adapter, oracle, config["rows"], "verify_reopen", control.deadline)
    io.resolve(retained, "replay_confirmed_identity", replay=True)
    verify_all(adapter, oracle, config["rows"], "verify_replay", control.deadline)
    sequential_bumps(io, oracle, 8, prepared + 16, "post_restart")
    verify_all(adapter, oracle, config["rows"], "verify_final", control.deadline)
    return {"status": "passed", "scenario": "C4", "confirmed_boundary": committed,
            "same_identity_replayed": True, "post_restart_writes": 8,
            "fault_scope": "three SIGKILL exits; OS and device remain running"}


def c5(adapter, root, config, control, cluster):
    _, oracle = prepare(adapter, root, config, control)
    io, network = FaultIO(adapter, cluster, control), Network(cluster, control)
    deadline = min(control.deadline, time.monotonic() + 60)
    leader = io.discover(deadline)
    target = next(node for node in (1, 2, 3) if node != leader["node_id"])
    initial = io.wait_status(target, lambda status: status["published_applied_index"] >=
                            leader["published_applied_index"], deadline)
    lower = initial["published_applied_index"]
    cluster.kill(target, deadline)
    io.excluded.add(target)
    count = 16 if config["variant"] == "short" else 256
    result = sequential_bumps(io, oracle, count, 0, "catchup_preparation")
    status = io.discover(min(control.deadline, time.monotonic() + 60))
    if config["variant"] == "long":
        while status["snapshot_base_index"] <= lower and count < 1024:
            result = sequential_bumps(io, oracle, 16, count, "catchup_preparation")
            count += 16
            status = io.discover(min(control.deadline, time.monotonic() + 60))
        if status["snapshot_base_index"] <= lower:
            raise NotCovered("no recovery base beyond stopped Follower within 1024 writes")
    elif status["snapshot_base_index"] > lower:
        raise NotCovered("short catch-up already requires snapshot coverage")
    boundary = result["committed_index"]
    since = time.monotonic_ns()
    deadline = min(control.deadline, time.monotonic() + 60)
    cluster.start(target, deadline)
    io.excluded.clear()
    caught_up = io.wait_status(target, lambda status: status["published_applied_index"] >= boundary, deadline)
    evidence = network.catchup(target, config["variant"], since, lower, deadline)
    # No new writes until this direct target check completes. STALE is explicit
    # and proves replica contents at a fixed boundary, not linearizable service.
    verify_all(adapter, oracle, config["rows"], "verify_target", control.deadline,
               endpoint=cluster.endpoint(target), stale=True)
    if config["variant"] == "short" and any(event["event"] == "forwarded" and event.get("type") == 5 and
                                             event.get("to") == target for event in network.events(since)):
        raise NotCovered("short path installed a snapshot during target verification")
    write_json(root / "catchup.json", {"variant": config["variant"], "target": target, "L": lower,
                                       "K": boundary, "target_published": caught_up["published_applied_index"],
                                       "confirmed_preparation_writes": count, "protocol": evidence,
                                       "target_business_verified": True})
    io.discover(min(control.deadline, time.monotonic() + 60))
    sequential_bumps(io, oracle, 8, count, "post_catchup")
    verify_all(adapter, oracle, config["rows"], "verify_final", control.deadline)
    return {"status": "passed", "scenario": "C5", "variant": config["variant"],
            "target": target, "recovered_to": boundary, "post_catchup_writes": 8}
