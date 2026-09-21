"""Distinct read/cache, write capacity, space turnover and recovery measurements."""

import json
import threading
import time

from adapter import RequestFailure, require_success, retryable
from arrival import ArrivalTraffic
from delivery import LogicalFailure
from content import Operation, bump, p1_operations, p2_operations, p3_groups
from faults import FaultIO, Network
from report import joined_history, records
from runtime import write_json
from workload import prepare, verify_all


def p4_foreground_budget(config):
    # Healthy/after-recovery traffic: 30s each. Pre-stop status and stopping:
    # 60s each. Formation, recovery, write barrier and drain have their own caps.
    return 180 + config["formation_seconds"] + config["recovery_seconds"] + 2 * config["request_seconds"]


def run_window(adapter, root, control, clients, seconds, phase, execute, *, writes=False):
    """A service error never shortens the denominator of a performance window."""
    start_gate, stop = threading.Event(), threading.Event()
    lock = threading.Lock()
    successes, failures, fatal, stalled = [0] * clients, [], [], {}
    start = adapter.driver.clock()
    python_start = time.monotonic_ns()
    measurement = {"start_ns": start, "end_ns": start + int(seconds * 1e9), "phase": phase,
                   "python_start_ns": python_start, "python_end_ns": python_start + int(seconds * 1e9),
                   "drain_deadline": min(control.deadline, time.monotonic() + seconds + adapter.request_seconds)}
    write_json(root / ("measurement.json" if phase == "measure" else f"{phase}.json"), measurement)

    def worker(index):
        start_gate.wait()
        try:
            while not stop.is_set() and time.monotonic_ns() < measurement["python_end_ns"]:
                control.check()
                try:
                    execute(index, measurement["end_ns"], measurement["drain_deadline"])
                    successes[index] += 1
                except LogicalFailure as error:
                    if error.outcome == "NOT_ISSUED":
                        return
                    with lock:
                        failures.append({"worker": index, "outcome": error.outcome, "error": str(error)})
                    if writes:
                        stalled[index] = time.monotonic_ns()
                        return  # Preserve the unresolved identity, never invent a new writer.
        except Exception as error:
            with lock:
                fatal.append(error)
            stop.set()

    threads = [threading.Thread(target=worker, args=(index,), name=f"{phase}-{index}", daemon=True)
               for index in range(clients)]
    try:
        for thread in threads:
            thread.start()
        start_gate.set()
        # Even if every writer parks, observe the entire declared window.
        while time.monotonic_ns() < measurement["python_end_ns"] and not stop.is_set():
            control.check()
            stop.wait(min(0.1, max(0, (measurement["python_end_ns"] - time.monotonic_ns()) / 1e9)))
        for thread in threads:
            while thread.is_alive():
                control.check(measurement["drain_deadline"] + 1)
                thread.join(timeout=0.05)
        if fatal:
            raise fatal[0]
    finally:
        stop.set()
        start_gate.set()
        for thread in threads:
            if thread.ident is not None:
                thread.join(timeout=1)
        if any(thread.is_alive() for thread in threads):
            control.fail("measurement worker did not stop; observation is incomplete")
    return {"status": "measured", "measurement_complete": True, "successes_by_client": successes,
            "failures": failures, "measurement": measurement,
            "stalled_client_seconds": {str(i): max(0, (measurement["python_end_ns"] - when) / 1e9)
                                       for i, when in stalled.items()}}


def prepare_performance(adapter, root, config, control):
    with adapter.stage("prepare", config["prepare_seconds"]) as deadline:
        rows, oracle = prepare(adapter, root, config, control)
        if config["scenario"] == "P2":
            for number in range(1, 1001):
                operation = bump((number - 1) % len(rows), number)
                adapter.write(operation, 3, "precondition", deadline)
                oracle.apply(operation)
            verify_all(adapter, oracle, len(rows), "verify_precondition", deadline)
    return rows, oracle


def finish_validation(adapter, oracle, rows, config, control, **read_options):
    try:
        with adapter.stage("validate", config["verify_seconds"]) as deadline:
            adapter.reconcile(oracle, deadline)
            verify_all(adapter, oracle, len(rows), "verify_final", deadline, **read_options)
        return {"business_validated": True, "validation": "passed"}
    except (LogicalFailure, TimeoutError) as error:
        return {"business_validated": False, "validation": "incomplete", "validation_error": str(error)}


def p1(adapter, root, config, control):
    rows, oracle = prepare_performance(adapter, root, config, control)
    results = {}
    for phase, seconds in (("warmup", 30), ("measure", config["seconds"])):
        streams = [p1_operations(len(rows), worker, config["seed"], config["variant"], phase) for worker in range(4)]

        def execute(index, cutoff, deadline):
            operation = next(streams[index])
            oracle.check(operation, adapter.read(operation, phase, deadline, cutoff_ns=cutoff))

        with adapter.stage(phase, seconds + config["request_seconds"] + 1):
            results[phase] = run_window(adapter, root, control, 4, seconds, phase, execute)
    return {**results["measure"], "scenario": "P1", "variant": config["variant"],
            "warmup": results["warmup"], **finish_validation(adapter, oracle, rows, config, control),
            "cache_scope": "fixed 30s warmup; report actual completions; OS/host caches included"}


def p2(adapter, root, config, control):
    rows, oracle = prepare_performance(adapter, root, config, control)
    clients = config["clients"]
    streams = [p2_operations(len(rows), clients, index, config["seed"]) for index in range(clients)]
    oracle_lock = threading.Lock()

    def execute(index, cutoff, deadline):
        operation = next(streams[index])
        adapter.write(operation, 100 + index, "measure", deadline, cutoff_ns=cutoff)
        with oracle_lock:
            oracle.apply(operation)

    with adapter.stage("measure", config["seconds"] + config["request_seconds"] + 1):
        result = run_window(adapter, root, control, clients, config["seconds"], "measure", execute, writes=True)
    return {**result, "scenario": "P2", **finish_validation(adapter, oracle, rows, config, control)}


def p3(adapter, root, config, control, cluster):
    rows, oracle = prepare_performance(adapter, root, config, control)
    baseline_bytes = sum(len(row.payload) for row in rows)
    groups = iter(p3_groups(rows, config["seed"]))
    total = advances = completed_rounds = 0
    windows = []
    previous_bases = []
    for node in (1, 2, 3):
        try:
            previous_bases.append(adapter.status(cluster.endpoint(node), min(control.deadline, time.monotonic() + 1))["snapshot_base_index"])
        except RequestFailure as error:
            if not retryable(error):
                raise
            previous_bases.append(None)
        except TimeoutError:
            previous_bases.append(None)
    begin_all = time.monotonic_ns()
    incomplete = None
    with adapter.stage("turnover", config["turnover_seconds"]) as deadline:
        with (root / "space_rounds.jsonl").open("x", buffering=1) as output:
            for round_number in range(1, 11):
                phase = f"turnover_{round_number}"
                begin = adapter.driver.clock()
                begin_python = time.monotonic_ns()
                before = cluster.storage_usage()
                done = 0
                try:
                    for _ in range(len(rows) // 20):
                        generated_round, operations = next(groups)
                        if generated_round != round_number:
                            raise AssertionError("P3 content round boundary changed")
                        for operation in operations:
                            control.check(deadline)
                            adapter.write(operation, 100, phase, deadline)
                            oracle.apply(operation)
                            done += 1
                            total += 1
                except (LogicalFailure, TimeoutError) as error:
                    incomplete = str(error)
                end_python = time.monotonic_ns()
                end = begin + end_python - begin_python
                bases = []
                for node in (1, 2, 3):
                    try:
                        bases.append(adapter.status(cluster.endpoint(node), min(deadline, time.monotonic() + 1))["snapshot_base_index"])
                    except RequestFailure as error:
                        if not retryable(error):
                            raise
                        bases.append(None)
                    except TimeoutError:
                        bases.append(None)
                advances += any(new is not None and old is not None and new > old
                                for new, old in zip(bases, previous_bases))
                previous_bases = bases
                complete = done == len(rows)
                if complete:
                    completed_rounds += 1
                    if len(oracle.rows) != len(rows) or sum(len(row.payload) for row in oracle.rows.values()) != baseline_bytes:
                        raise AssertionError("P3 round changed live rows/payload bytes")
                record = {"round": round_number, "phase": phase, "complete": complete,
                          "start_ns": begin, "end_ns": end, "python_start_ns": begin_python,
                          "python_end_ns": end_python, "completed_operations": done,
                          "cumulative_operations": total, "live_rows": len(oracle.rows),
                          "logical_payload_bytes": sum(len(row.payload) for row in oracle.rows.values()),
                          "before": before, "after": cluster.storage_usage(), "snapshot_bases": bases,
                          "observed_retention_advances": advances}
                output.write(json.dumps(record, sort_keys=True) + "\n")
                windows.append(record)
                write_json(root / "measurement_windows.json", windows)
                if incomplete:
                    # A timed-out write blocks its dependent suffix. Continue
                    # observing space to the scenario limit, without fake work.
                    while time.monotonic() < deadline:
                        control.check()
                        time.sleep(min(0.1, max(0, deadline - time.monotonic())))
                    break
                try:
                    verify_all(adapter, oracle, len(rows), f"verify_round_{round_number}", deadline)
                except (LogicalFailure, TimeoutError) as error:
                    incomplete = str(error)
                    break
    observed_seconds = (time.monotonic_ns() - begin_all) / 1e9
    write_json(root / "turnover_progress.json", {"target_operations": len(rows) * 10,
               "completed_operations": total, "completed_rounds": completed_rounds,
               "observed_seconds": observed_seconds, "completion_censored": total < len(rows) * 10,
               "interruption": incomplete})
    if total == len(rows) * 10:
        with adapter.stage("idle", 61):
            wait_end = time.monotonic() + 60
            while time.monotonic() < wait_end:
                control.check()
                time.sleep(min(0.1, max(0, wait_end - time.monotonic())))
        write_json(root / "space_after_idle.json", {"idle_seconds": 60, "storage_usage": cluster.storage_usage()})
    return {"status": "measured", "scenario": "P3", "measurement_complete": total == len(rows) * 10,
            "rounds": completed_rounds, "completed_operations": total, "target_operations": len(rows) * 10,
            "completion_seconds": observed_seconds if total == len(rows) * 10 else None,
            "completion_lower_bound_seconds": observed_seconds if total < len(rows) * 10 else None,
            **finish_validation(adapter, oracle, rows, config, control)}


def apply_foreground(root, oracle):
    """Check successful logical operations once, including post-window resolution.

    A failed/late transport attempt is not a second business mutation. Reachable
    row checks are separate from the real-time ordering checks in C2/C3.
    """
    logical_path = root / "logical_history.jsonl"
    accepted = None
    if logical_path.exists():
        accepted = {event["logical_id"] for event in records(logical_path)
                    if event["event"] == "end" and event["outcome"] == "SUCCESS"}
    versions = {(key, str(row.version)): str(row.last_op) for key, row in oracle.rows.items()}
    seen = set()
    for item, event in joined_history(root):
        if item["phase"] not in ("foreground", "reconcile") or item["kind"] != "WRITE" or event["event"] == "call":
            continue
        logical_id = item.get("logical_id", item["correlation"])
        if logical_id in seen or (accepted is not None and logical_id not in accepted):
            continue
        try:
            require_success(event, write=True)
        except RequestFailure:
            continue
        seen.add(logical_id)
        operation = Operation(**item["operation"])
        oracle.apply(operation)
        row = oracle.rows[operation.args["id"]]
        versions[(row.id, str(row.version))] = str(row.last_op)
    for item, event in joined_history(root):
        if item["phase"] != "foreground" or item["kind"] != "READ" or event["event"] == "call":
            continue
        try:
            require_success(event, write=False)
        except RequestFailure:
            continue
        actual = event.get("rows", [])
        key = item["operation"]["args"]["id"]
        final = oracle.rows[key].values()
        if (len(actual) != 1 or len(actual[0]) != 6 or actual[0][:3] != final[:3] or
                actual[0][5] != final[5] or versions.get((key, actual[0][3])) != actual[0][4]):
            raise AssertionError("foreground read is not a complete reachable business row")


def p4(adapter, root, config, control, cluster):
    rows, oracle = prepare_performance(adapter, root, config, control)
    io, network = FaultIO(adapter, cluster, control), Network(cluster, control)
    leader = io.discover(min(control.deadline, time.monotonic() + 60))
    target = next(node for node in (1, 2, 3) if node != leader["node_id"])
    io.wait_status(target, lambda status: status["published_applied_index"] >= leader["published_applied_index"],
                   min(control.deadline, time.monotonic() + 60))
    traffic = ArrivalTraffic(adapter, rows, root, config, control)
    timeline = {"target": target}
    complete = False
    interrupted = None
    progress_log = (root / "recovery_progress.jsonl").open("x", buffering=1)

    def observe(status):
        _, last_confirmed = traffic.progress()
        progress_log.write(json.dumps({"python_ns": time.monotonic_ns(),
                                       "target_published": status["published_applied_index"],
                                       "last_confirmed_index": last_confirmed,
                                       "behind_confirmed": max(0, last_confirmed - status["published_applied_index"])}) + "\n")

    with adapter.stage("foreground", p4_foreground_budget(config)):
        try:
            traffic.start()
            timeline["start_ns"] = traffic.start_ns
            traffic.wait_until(time.monotonic() + 30)
            initial = io.wait_status(target, lambda status: True, min(control.deadline, time.monotonic() + 60))
            lower = initial["published_applied_index"]
            timeline["stop_follower_ns"] = adapter.driver.clock()
            cluster.kill(target, min(control.deadline, time.monotonic() + 60))
            stopped_count, _ = traffic.progress()
            formation_end = min(control.deadline, time.monotonic() + config["formation_seconds"])
            while traffic.progress()[0] - stopped_count < 256:
                traffic.check(formation_end)
                time.sleep(0.05)
            timeline["write_barrier_start_ns"] = adapter.driver.clock()
            confirmed, boundary = traffic.pause_writes(min(control.deadline, time.monotonic() + config["request_seconds"]))
            if adapter.pending:
                raise TimeoutError("cannot establish K0 while foreground writes are unconfirmed")
            timeline.update(L=lower, K0=boundary, offline_confirmed_writes=confirmed - stopped_count)
            since = time.monotonic_ns()
            timeline["restart_issued_ns"] = adapter.driver.clock()
            deadline = min(control.deadline, time.monotonic() + config["recovery_seconds"])
            cluster.start(target, deadline)
            timeline["process_start_ack_ns"] = adapter.driver.clock()
            traffic.resume_writes()
            timeline["write_barrier_end_ns"] = adapter.driver.clock()
            io.wait_status(target, lambda status: True, deadline, observe=observe)
            timeline["target_status_ready_ns"] = adapter.driver.clock()
            caught = io.wait_status(target, lambda status: status["published_applied_index"] >= boundary, deadline,
                                   observe=observe)
            timeline["recovered_k0_ns"] = adapter.driver.clock()
            timeline["target_published_at_k0"] = caught["published_applied_index"]
            timeline["protocol"] = network.catchup(target, "either", since, lower, deadline)
            traffic.wait_until(time.monotonic() + 30)
            complete = True
        except (LogicalFailure, TimeoutError) as error:
            interrupted = str(error)
            timeline["interruption"] = interrupted
            timeline["observation_ended_ns"] = adapter.clock_ns + time.monotonic_ns() - adapter.python_ns
            timeline["coverage"] = ("recovery_unconfirmed" if "restart_issued_ns" in timeline
                                    else "precondition_incomplete")
        finally:
            try:
                if hasattr(traffic, "start_ns"):
                    traffic.close()
            finally:
                progress_log.close()
                if hasattr(traffic, "end_ns"):
                    timeline["end_ns"] = traffic.end_ns
                    write_json(root / "measurement.json", {"phase": "foreground", "start_ns": traffic.start_ns,
                               "end_ns": traffic.end_ns, "python_start_ns": traffic.python_start_ns,
                               "python_end_ns": traffic.end_python_ns})
                write_json(root / "recovery_timeline.json", timeline)
    traffic.check()
    validated = False
    validation_error = None
    try:
        with adapter.stage("validate", config["verify_seconds"]) as deadline:
            # Reconcile only for state checking. Rebuild the oracle from the
            # logical success journal to avoid counting any retry twice.
            reconciled_index = adapter.reconcile(None, deadline, apply=False)
            apply_foreground(root, oracle)
            if complete:
                _, final_boundary = traffic.progress()
                final_boundary = max(final_boundary, reconciled_index)
                io.wait_status(target, lambda status: status["published_applied_index"] >= final_boundary, deadline)
                verify_all(adapter, oracle, len(rows), "verify_target", deadline,
                           endpoint=cluster.endpoint(target), stale=True)
            verify_all(adapter, oracle, len(rows), "verify_final", deadline)
            validated = True
    except (LogicalFailure, TimeoutError) as error:
        validation_error = str(error)
    dispatch = json.loads((root / "arrival_dispatch.json").read_text())
    recovered = "recovered_k0_ns" in timeline
    return {"status": "measured", "scenario": "P4", "rate": config["rate"],
            "measurement_complete": complete, "business_validated": validated,
            "validation": "passed" if validated else "incomplete", "validation_error": validation_error,
            "coverage": "recovered" if complete else timeline.get("coverage", "incomplete"),
            "interruption": interrupted, "arrival_dispatch": dispatch,
            "recovery_seconds": ((timeline["recovered_k0_ns"] - timeline["restart_issued_ns"]) / 1e9
                                 if recovered else None),
            "recovery_lower_bound_seconds": ((timeline["observation_ended_ns"] - timeline["restart_issued_ns"]) / 1e9
                                            if not recovered and "restart_issued_ns" in timeline else None)}
