#!/usr/bin/env python3
"""Opt-in external sampling of one existing acceptance run; never starts nodes."""

import argparse
import json
import signal
import subprocess
import time
from pathlib import Path


def events(path, offsets):
    if not path.exists():
        return
    with path.open() as source:
        source.seek(offsets.get(path, 0))
        while True:
            line = source.readline()
            if not line.endswith("\n"):
                return  # Retry an incompletely published line on the next poll.
            offsets[path] = source.tell()
            yield json.loads(line)


def capture(args, nodes, trigger, number):
    root = args.output / f"capture-{number}"
    root.mkdir()
    # Only attach to the nodes recorded by this run, still using its data paths.
    for pid in nodes.values():
        command = (Path("/proc") / str(pid) / "cmdline").read_bytes().split(b"\0")
        if not any(str(args.run / "nodes").encode() in part for part in command):
            raise RuntimeError(f"PID {pid} no longer belongs to this run")
    pids = ",".join(str(pid) for pid in nodes.values())
    commands = {"pidstat": ["pidstat", "-h", "-t", "-u", "-r", "-d", "-w", "-p", pids,
                                      "1", str(args.seconds)]}
    if not args.no_perf:
        commands["perf"] = [str(args.perf), "record", "-e", "cpu-clock:u", "-F", "49",
                                      "--call-graph", "dwarf,8192", "--clockid", "mono", "-p", pids,
                                      "-o", str(root / "perf.data"), "--", "sleep", str(args.seconds)]
    record = {"trigger": trigger, "nodes": nodes, "started_monotonic_ns": time.monotonic_ns(),
              "commands": commands, "exit_codes": {}, "diagnostic_only": True}
    children, logs = {}, []
    try:
        for name, command in commands.items():
            log = (root / f"{name}.log").open("wb")
            logs.append(log)
            children[name] = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        if args.stacks:
            # ptrace briefly stops the node: these timings are not baseline data.
            for node, pid in nodes.items():
                command = ["gdb", "-nx", "-batch", "-iex", "set auto-load off",
                                    "-iex", "set debuginfod enabled off", "-ex", "set pagination off",
                                    "-ex", "thread apply all bt 16", "-ex", "detach", "-p", str(pid)]
                record["commands"][f"stack-{node}"] = command
                with (root / f"stack-{node}.log").open("wb") as log:
                    try:
                        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=20)
                        record["exit_codes"][f"stack-{node}"] = result.returncode
                    except subprocess.TimeoutExpired:
                        record["exit_codes"][f"stack-{node}"] = "timeout"
        for name, child in children.items():
            record["exit_codes"][name] = child.wait(timeout=args.seconds + 30)
    finally:
        for child in children.values():
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        for log in logs:
            log.close()
        record["ended_monotonic_ns"] = time.monotonic_ns()
        (root / "capture.json").write_text(json.dumps(record, indent=2) + "\n")
    return all(code == 0 for code in record["exit_codes"].values())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True, help="one case directory, may not exist yet")
    parser.add_argument("--output", type=Path, required=True, help="new diagnostic directory")
    parser.add_argument("--stop-file", type=Path, help="stop waiting when the owning runner creates this file")
    parser.add_argument("--when", choices=("measure", "stall", "restart"), required=True)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--stall-seconds", type=int, default=10)
    parser.add_argument("--captures", type=int, default=1)
    parser.add_argument("--wait-seconds", type=int, default=6600)
    parser.add_argument("--perf", type=Path, default=Path("/usr/lib/linux-tools/6.8.0-142-generic/perf"))
    parser.add_argument("--no-perf", action="store_true", help="explicitly collect only pidstat/optional stacks")
    parser.add_argument("--stacks", action="store_true", help="also briefly stop each node for a GDB backtrace")
    args = parser.parse_args()
    if min(args.seconds, args.stall_seconds, args.captures, args.wait_seconds) <= 0:
        parser.error("durations and counts must be positive")
    args.run = args.run.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    offsets, nodes, pending, captured = {}, {}, {}, set()
    stage, restart = None, None
    count, ok = 0, True
    deadline = time.monotonic() + args.wait_seconds
    while time.monotonic() < deadline:
        if args.stop_file and args.stop_file.exists():
            break
        for item in events(args.run / "faults.jsonl", offsets):
            if item["event"] == "started":
                nodes = item["nodes"]
            elif item["event"] == "exit_waited":
                nodes.pop(str(item["node"]), None)
            elif item["event"] == "restart_acknowledged":
                nodes[str(item["node"])] = item["pid"]
                restart = item
        for item in events(args.run / "stages.jsonl", offsets):
            stage = item["stage"] if item["event"] == "start" else None
        if args.when == "stall":
            for item in events(args.run / "logical_history.jsonl", offsets):
                if item["event"] == "start" and item["phase"].startswith("turnover_"):
                    pending[item["logical_id"]] = item
                elif item["event"] == "end":
                    pending.pop(item["logical_id"], None)
        trigger = None
        if args.when == "measure" and stage == "measure":
            trigger = {"kind": "measure", "id": "measure"}
        elif args.when == "restart" and restart:
            trigger = {"kind": "restart", "id": f"restart-{restart['pid']}", "event": restart}
        elif args.when == "stall":
            for identity, item in pending.items():
                if identity not in captured and time.monotonic_ns() - item["started_python_ns"] >= args.stall_seconds * 10**9:
                    trigger = {"kind": "slow_turnover_request", "id": identity, "request": item}
                    break
        if trigger and trigger["id"] not in captured and nodes:
            captured.add(trigger["id"])
            count += 1
            ok = capture(args, nodes, trigger, count) and ok
            print(f"capture {count}: {trigger['kind']} -> {args.output}", flush=True)
            if count == args.captures:
                break
        if (args.run / "result.json").exists():
            break
        time.sleep(0.5)
    (args.output / "summary.json").write_text(json.dumps({"captures": count, "tools_succeeded": ok,
        "status": "captured" if count else "not_triggered", "diagnostic_only": True}, indent=2) + "\n")
    return 0 if count and ok else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
