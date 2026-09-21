#!/usr/bin/env python3
"""Opt-in storage correctness/performance runs. No implicit build, execution on import, or artifact deletion."""

import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback

from adapter import ADAPTER_VERSION, Adapter, RequestFailure
from content import C1_WORKLOAD_VERSION, CONTENT_VERSION, PERFORMANCE_VERSION, SEED
from report import summarize, summarize_arrivals, summarize_resources, summarize_space, summarize_logical, summarize_runs
from delivery import LogicalFailure, PerformanceIO
from runtime import (Cluster, Control, Driver, EnvironmentLimit, GIB, Monitor, memory_sample,
                     write_json)
from correctness import c1, c2, c3, c4, c5
from faults import NotCovered
from history import check_history
from performance import p1, p2, p3, p4, p4_foreground_budget


REPO = Path(__file__).resolve().parents[2]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for data in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(data)
    return digest.hexdigest()


def git(*args):
    return subprocess.check_output(["git", "-C", str(REPO), *args], text=True)


def preflight(args):
    if not sys.platform.startswith("linux") or not Path("/proc/diskstats").exists():
        raise EnvironmentLimit("acceptance resource sampler currently requires Linux /proc")
    if not 1024 <= args.port_base <= 65234:
        raise ValueError("port base must leave node, proxy and response-drop ports valid")
    if args.max_seconds <= args.seconds + 60 + (30 if args.scenario == "P1" else 0) and args.scenario in ("P1", "P2"):
        raise ValueError("scenario deadline must leave preparation and at least 60 seconds of drain")
    if not math.isfinite(args.seconds) or args.seconds <= 0 or args.max_seconds <= 0:
        raise ValueError("time budgets must be positive")
    if args.output.exists() or not args.output.parent.is_dir():
        raise ValueError("output must be a new directory under an existing parent")
    if shutil.disk_usage(args.output.parent).free < 11 * GIB:
        raise EnvironmentLimit("less than 10 GiB artifact budget plus 1 GiB filesystem headroom")
    if memory_sample()["MemAvailable"] < GIB:
        raise EnvironmentLimit("less than 1 GiB MemAvailable")
    if "microsoft" in platform.release().lower() and args.host_free_gib is None:
        raise EnvironmentLimit("WSL needs --host-free-gib from a separate host-disk check; guest df is insufficient")
    if args.host_free_gib is not None and (not math.isfinite(args.host_free_gib) or args.host_free_gib < 11):
        raise EnvironmentLimit("host free-space evidence is below the 11 GiB starting requirement")
    binaries = [args.build / "bin/bustub-node", args.build / "bin/bustub-client",
                args.build / "test/storage-e2e-driver"]
    for binary in binaries:
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise EnvironmentLimit(f"build the reviewed target first: missing {binary}")
    if args.scenario in ("C2", "C3") and (args.checker is None or not args.checker.is_file() or
                                               not os.access(args.checker, os.X_OK)):
        raise EnvironmentLimit("C2/C3 require the reviewed, separately built --checker binary")
    cache = args.build / "CMakeCache.txt"
    if not cache.is_file():
        raise EnvironmentLimit("build metadata CMakeCache.txt is required")
    if "CMAKE_BUILD_TYPE:STRING=Release" not in cache.read_text():
        raise EnvironmentLimit("use the same Release build for this E2E/performance suite")
    reservations = []
    try:
        for port in [args.port_base + offset for offset in (1, 2, 3, 101, 102, 103, 201, 202, 203, 301)]:
            sock = socket.socket()
            reservations.append(sock)
            # Match the production listeners: a previous owned run's TIME_WAIT
            # connections do not mean another server is listening on this port.
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", port))
    finally:
        for sock in reservations:
            sock.close()
    # Probing does not reserve ports through node startup; a later bind race is a failure, never an automatic retry.


def save_environment(args):
    root = args.output
    write_json(root / "environment.json", {
        "format_version": 1, "utc_start": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "git_head": git("rev-parse", "HEAD").strip(), "git_status": git("status", "--short"),
        "platform": platform.platform(), "python": sys.version, "logical_cpus": os.cpu_count(),
        "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"), "page_size": os.sysconf("SC_PAGE_SIZE"),
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "cpuinfo": Path("/proc/cpuinfo").read_text(), "memory": memory_sample(),
        "build": str(args.build), "host_free_gib_supplied": args.host_free_gib,
        "filesystem_free_bytes": shutil.disk_usage(root).free,
        "budget_mode": "observed stop thresholds, not cgroup enforcement",
        "io_scope": "process counters and host-wide device counters; observer IO is included on shared devices",
        "fault_scope": "C1/C2/P1/P2/P3 healthy; C3-C5/P4 process/link/response faults only; no power-loss evidence",
        "checker_binary": str(args.checker) if args.checker else None,
        "checker_sha256": sha256(args.checker) if args.checker else None,
    })
    (root / "source.diff").write_text(git("diff", "--binary", "HEAD"))
    if args.rate_source:
        shutil.copyfile(args.rate_source, root / "frozen-rate-source.json")
    shutil.copyfile(args.build / "CMakeCache.txt", root / "CMakeCache.txt")
    if (args.build / "compile_commands.json").is_file():
        shutil.copyfile(args.build / "compile_commands.json", root / "compile_commands.json")
    files = [p for p in Path(__file__).parent.rglob("*") if p.is_file() and "__pycache__" not in p.parts]
    files += [*sorted((REPO / "test/support").glob("*.py")),
              REPO / "test/support/raft_process_harness.sh", REPO / "test/CMakeLists.txt"]
    archive = root / "test-source"
    archive.mkdir()
    hashes = {}
    for path in files:
        relative = path.relative_to(REPO)
        target = archive / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
        hashes[str(relative)] = sha256(path)
    write_json(root / "source-hashes.json", hashes)
    write_json(root / "binaries.json", {str(path.relative_to(args.build)): sha256(path) for path in
                                      (args.build / "bin/bustub-node", args.build / "bin/bustub-client",
                                       args.build / "test/storage-e2e-driver")})


def run_one(args, config, root):
    root.mkdir()
    encoded = json.dumps(config, sort_keys=True, separators=(",", ":"))
    write_json(root / "config.json", {**config, "config_sha256": hashlib.sha256(encoded.encode()).hexdigest()})
    control = Control(config["max_seconds"])
    driver = cluster = monitor = delivery = None
    result = {"status": "incomplete", "scenario": config["scenario"], "role": config["role"]}
    started = time.monotonic()
    try:
        cluster = Cluster(args.build, root, config)
        driver = Driver(args.build / "test/storage-e2e-driver", root, control)
        monitor = Monitor(root, args.output, cluster, driver.process.pid, control)
        endpoints = [f"127.0.0.1:{config['port_base'] + 100 + node}" for node in (1, 2, 3)]
        adapter = Adapter(driver, endpoints)
        adapter.discover(min(control.deadline, time.monotonic() + 60))
        if config["scenario"].startswith("P"):
            adapter = delivery = PerformanceIO(adapter, root, config, control)
        if config["scenario"] in ("C1", "P1", "P2"):
            result.update({"C1": c1, "P1": p1, "P2": p2}[config["scenario"]](adapter, root, config, control))
        else:
            result.update({"C2": c2, "C3": c3, "C4": c4, "C5": c5, "P3": p3, "P4": p4}[config["scenario"]](
                adapter, root, config, control, cluster))
        control.check()
    except LogicalFailure as error:
        stage = delivery.stage_name if delivery else "startup"
        # The context restored its parent; retain the persisted stage boundary.
        if (root / "current_stage.json").exists():
            stage = json.loads((root / "current_stage.json").read_text())["stage"]
        result.update(status="observation_incomplete", outcome=error.outcome, error=str(error),
                      incomplete_stage=stage, business_validated=False, measurement_complete=False)
    except NotCovered as error:
        result.update(status="not_covered", error=str(error))
    except EnvironmentLimit as error:
        result.update(status="environment_limited", error=str(error))
    except RequestFailure as error:
        result.update(status="request_failure", outcome=error.outcome, error=str(error))
    except TimeoutError as error:
        result.update(status="deadline_exceeded", error=str(error))
    except AssertionError as error:
        result.update(status="correctness_failure", error=str(error))
    except Exception as error:
        result.update(status="harness_or_environment_failure", error=str(error), traceback=traceback.format_exc())
    finally:
        cleanup = {}
        if delivery:
            delivery.close()
        for name, resource in (("monitor_stopped", monitor), ("driver", driver), ("cluster_exit", cluster)):
            if resource:
                try:
                    cleanup[name] = (resource.close(abort=result["status"] not in ("passed", "measured")) if name == "driver"
                                     else resource.close())
                except Exception as error:
                    cleanup[f"{name}_error"] = str(error)
        result["cleanup"] = cleanup
        result["elapsed_seconds"] = time.monotonic() - started
        if result["status"] in ("passed", "measured") and control.failure:
            result.update(status="environment_limited", error=control.failure)
        if result["status"] in ("passed", "measured") and (
                cleanup.get("cluster_exit") != 0 or not cleanup.get("monitor_stopped") or
                cleanup.get("driver", {}).get("exit_code") != 0 or
                cleanup.get("driver", {}).get("history_error") or
                not cleanup.get("driver", {}).get("reader_stopped")):
            result["status"] = "cleanup_or_history_failure"
        write_json(root / "result.json", result)
    try:
        measurement = json.loads((root / "measurement.json").read_text()) if (root / "measurement.json").exists() else None
        if (root / "inputs.jsonl").exists() and (root / "history.jsonl").exists():
            result["attempt_summary"] = summarize(root, measurement, measurement.get("phase", "measure") if measurement else "measure")
        if (root / "resources.jsonl").exists():
            result["resource_summary"] = summarize_resources(root, measurement)
        if (root / "logical_history.jsonl").exists():
            result["logical_summary"] = summarize_logical(root, measurement)
            result["service_outcomes"] = result["logical_summary"]["phase_outcomes"]
        if (root / "space_rounds.jsonl").exists():
            result["space_summary"] = summarize_space(root)
            if result["status"] == "passed" and result["space_summary"]["status"] == "not_covered":
                result["status"] = "not_covered"
        if (root / "recovery_timeline.json").exists() and (root / "arrivals.jsonl").exists():
            result["arrival_summary"] = summarize_arrivals(root)
    except EnvironmentLimit as error:
        result.update(report_status="environment_limited", report_error=str(error))
        if result["status"] in ("passed", "measured"):
            result["status"] = "environment_limited"
    except Exception as error:
        result.update(report_status="report_failure", report_error=str(error), report_traceback=traceback.format_exc())
        if result["status"] in ("passed", "measured"):
            result["status"] = "report_failure"
    if (root / "history_initial.json").exists():
        if not (root / "history_boundary.json").exists():
            result["linearizability"] = {"result": "Incomplete", "reason": "no completed observation boundary"}
            if result["status"] == "passed":
                result["status"] = "check_incomplete"
        else:
            try:
                checker_started = time.monotonic()
                checked = check_history(root, args.checker)
                result["offline_checker_seconds"] = time.monotonic() - checker_started
                result["linearizability"] = checked
                if result["status"] == "passed":
                    if checked["result"] == "Illegal":
                        result["status"] = "correctness_failure"
                    elif checked["result"] != "Ok":
                        result["status"] = "check_incomplete"
                    else:
                        coverage = checked["coverage"]
                        if (not coverage["successful_writes"] or not coverage["successful_reads"] or
                                (result.get("requires_overlap") and not coverage["actual_network_overlap"]) or
                                (result.get("requires_post_fault") and coverage["post_fault_successes"] < 8)):
                            result["status"] = "not_covered"
            except Exception as error:
                result["linearizability"] = {"result": "Incomplete", "error": str(error)}
                if result["status"] == "passed":
                    result["status"] = "check_incomplete"
    if config["scenario"].startswith("P"):
        result["measurement_valid"] = (result["status"] == "measured" and result.get("measurement_complete", False)
                                       and result.get("logical_summary", {}).get("observation_complete", True))
        result["validated_comparison"] = result["measurement_valid"] and result.get("business_validated", False)
        result["execution_coverage"] = {"measurement_complete": result.get("measurement_complete", False),
                                        "incomplete_stage": result.get("incomplete_stage"),
                                        "completed_operations": result.get("completed_operations"),
                                        "target_operations": result.get("target_operations")}
    write_json(root / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=("C1", "C2", "C3", "C4", "C5", "P1", "P2", "P3", "P4"))
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new directory; never overwritten or deleted")
    parser.add_argument("--role", choices=("qualification", "baseline"), required=True)
    parser.add_argument("--port-base", type=int, required=True)
    parser.add_argument("--clients", type=int, choices=(1, 2, 4, 8), default=1)
    parser.add_argument("--matrix", action="store_true", help="P1 distribution/repeats; P2 concurrency/repeats; C2 seeds; C3/C5 variants")
    parser.add_argument("--variant", choices=("kill", "isolate", "drop", "short", "long", "uniform", "hotspot"))
    parser.add_argument("--checker", type=Path, help="separately built Porcupine checker for C2/C3")
    parser.add_argument("--repeat", type=int, choices=(1, 2, 3), default=1)
    parser.add_argument("--seconds", type=float, default=180)
    parser.add_argument("--max-seconds", type=int, help="outer watchdog; performance default is the sum of independent stage budgets")
    parser.add_argument("--rate-source", type=Path, help="P4: frozen, validated old P2 one-client qualification result.json (errors allowed)")
    parser.add_argument("--fixed-rate", type=float, help="P4 explicit frozen absolute rate; default 1 op/s without rate source")
    parser.add_argument("--request-seconds", type=float, default=60)
    parser.add_argument("--prepare-seconds", type=float, default=1800)
    parser.add_argument("--verify-seconds", type=float, default=600)
    parser.add_argument("--turnover-seconds", type=float, default=3600)
    parser.add_argument("--formation-seconds", type=float, default=1800)
    parser.add_argument("--recovery-seconds", type=float, default=600)
    parser.add_argument("--host-free-gib", type=float, help="separately checked host free space (required on WSL)")
    args = parser.parse_args()
    args.build = args.build.resolve()
    args.output = args.output.absolute()
    if args.seconds != 180 and args.scenario not in ("P1", "P2"):
        parser.error("--seconds changes only P1/P2 windows; P3/P4 use their fixed scenario timelines")
    if args.clients != 1 and args.scenario != "P2":
        parser.error("--clients selects only P2 concurrency; other scenarios have fixed worker counts")
    budgets = (args.request_seconds, args.prepare_seconds, args.verify_seconds,
               args.turnover_seconds, args.formation_seconds, args.recovery_seconds)
    if any(not math.isfinite(value) or value <= 0 for value in budgets):
        parser.error("stage/request budgets must be finite positive seconds")
    if args.scenario.startswith("P"):
        execution = {"P1": 30 + args.seconds + 2 * (args.request_seconds + 1),
                     "P2": args.seconds + args.request_seconds + 1,
                     "P3": args.turnover_seconds + 61,
                     "P4": p4_foreground_budget(vars(args))}[args.scenario]
        minimum_outer = math.ceil(120 + args.prepare_seconds + args.verify_seconds + execution)
        if args.max_seconds is not None and args.max_seconds < minimum_outer:
            parser.error(f"outer budget must cover independent stages ({minimum_outer}s); select stage budgets explicitly")
        args.max_seconds = args.max_seconds or minimum_outer
    else:
        args.max_seconds = args.max_seconds or 600
    args.rate = None
    args.rate_source_sha256 = None
    args.rate_mode = None
    if args.scenario == "P4":
        if args.rate_source and args.fixed_rate is not None:
            parser.error("select a frozen rate source OR an explicit fixed rate")
        if args.rate_source:
            args.rate_source = args.rate_source.resolve()
            source = json.loads(args.rate_source.read_text())
            if (source.get("scenario") != "P2" or not source.get("validated_comparison") or
                    source.get("role") != "qualification" or len(source.get("successes_by_client", [])) != 1):
                parser.error("rate source must be a fully observed and business-validated one-client P2 qualification")
            args.rate = source["logical_summary"]["success_ops_per_second"] * 0.5
            args.rate_source_sha256 = sha256(args.rate_source)
            args.rate_mode = "half_frozen_old_logical_goodput"
        else:
            args.rate = args.fixed_rate if args.fixed_rate is not None else 1.0
            args.rate_mode = "fixed_absolute"
        if not math.isfinite(args.rate) or args.rate <= 0:
            parser.error("P4 requires a finite positive offered rate")
    elif args.rate_source or args.fixed_rate is not None:
        parser.error("rate selection applies only to P4")
    if args.checker:
        args.checker = args.checker.resolve()
    if args.matrix and args.scenario not in ("P1", "P2", "C2", "C3", "C5"):
        parser.error("--matrix applies to P1/P2/C2/C3/C5")
    allowed = {"C3": ("kill", "isolate", "drop"), "C5": ("short", "long"), "P1": ("uniform", "hotspot")}
    if args.scenario in allowed:
        if args.matrix and args.variant:
            parser.error("select either the complete variant matrix or one --variant")
        if not args.matrix and args.variant not in allowed[args.scenario]:
            parser.error(f"{args.scenario} requires --variant from {allowed[args.scenario]}")
    elif args.variant:
        parser.error("--variant applies only to C3/C5/P1")
    if args.role == "qualification" and (args.matrix or (not args.scenario.startswith("P") and args.max_seconds > 600)):
        parser.error("qualification is one point; correctness qualification retains its 600-second ceiling")
    lock_path = Path(tempfile.gettempdir()) / f"bustub-storage-e2e-{os.getuid()}.lock"
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        preflight(args)
        args.output.mkdir()
        save_environment(args)
        if args.matrix and args.scenario == "P2":
            points = [(clients, repeat, None) for clients in (1, 2, 4, 8) for repeat in (1, 2, 3)]
        elif args.matrix and args.scenario == "P1":
            points = [(4, repeat, variant) for variant in allowed["P1"] for repeat in (1, 2, 3)]
        elif args.matrix and args.scenario == "C2":
            points = [(4, repeat, None) for repeat in (1, 2, 3)]
        elif args.matrix:
            points = [(4 if args.scenario == "C3" else 1, 1, variant) for variant in allowed[args.scenario]]
        else:
            points = [(args.clients if args.scenario == "P2" else 4 if args.scenario in ("C2", "C3", "P1") else
                       6 if args.scenario == "P4" else 1,
                       args.repeat, args.variant)]
        results = []
        for clients, repeat, variant in points:
            config = {"format_version": 1, "content_version": CONTENT_VERSION, "adapter_version": ADAPTER_VERSION,
                      "scenario": args.scenario, "variant": variant, "role": args.role,
                      "c1_workload_version": C1_WORKLOAD_VERSION if args.scenario == "C1" else None,
                      "performance_version": PERFORMANCE_VERSION if args.scenario.startswith("P") else None,
                      "rate": args.rate, "rate_mode": args.rate_mode, "rate_source_sha256": args.rate_source_sha256,
                      "request_seconds": args.request_seconds, "prepare_seconds": args.prepare_seconds,
                      "verify_seconds": args.verify_seconds, "turnover_seconds": args.turnover_seconds,
                      "formation_seconds": args.formation_seconds, "recovery_seconds": args.recovery_seconds,
                      "seed": SEED + repeat - 1 if args.scenario == "C2" else SEED,
                      "rows": 20000 if args.scenario.startswith("P") else 2048, "buffer_pages": 256,
                      "snapshot_threshold": 64 if args.scenario == "C4" or variant == "long" else 10000,
                      "proxies": args.scenario in ("C5", "P4") or variant == "isolate", "port_base": args.port_base,
                      "clients": 1 if variant == "drop" else clients, "repeat": repeat,
                      "seconds": args.seconds, "max_seconds": args.max_seconds,
                      "node_timeout_ms": 5000, "client_timeout_ms": 6000,
                      "precondition_writes": 1000 if args.scenario == "P2" else 0}
            name = f"{args.scenario}{'-' + variant if variant else ''}-c{config['clients']}-r{repeat}"
            print(f"starting {name}; artifacts: {args.output / name}", flush=True)
            result = run_one(args, config, args.output / name)
            results.append({"name": name, "clients": config["clients"], "repeat": repeat, "variant": variant, **result})
            write_json(args.output / "runs.json", results)
            print(f"{name}: {result['status']}", flush=True)
            # Preserve failures; do not replace them with extra repetitions. A broken environment ends the batch.
            if result["status"] in ("environment_limited", "harness_or_environment_failure", "cleanup_or_history_failure"):
                break
        write_json(args.output / "summary.json", summarize_runs(
            results, len(points), args.role, args.scenario, 3 if args.matrix else 1))
        # Exit 0 means the observation plan completed, not zero service errors.
        # A bounded but incomplete workload exits 1 and still retains its metrics.
        return 0 if len(results) == len(points) and all(item.get("validated_comparison", item["status"] == "passed")
                                                       for item in results) else 1
    finally:
        os.close(lock_fd)


def interrupted(signum, _frame):
    raise InterruptedError(f"runner received signal {signum}; preserve artifacts and stop owned processes")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        sys.exit(main())
    except Exception as error:
        print(f"storage-e2e: {error}", file=sys.stderr)
        sys.exit(2)
