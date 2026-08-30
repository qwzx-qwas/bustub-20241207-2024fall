#!/usr/bin/env python3

"""Run every M0-M8 component binary once and prove that it executed tests."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


TEST_BINARIES = (
    "versioned_frame_test",
    "state_manifest_test",
    "table_heap_reopen_test",
    "canonical_snapshot_test",
    "log_codec_test",
    "snapshot_manager_test",
    "power_loss_storage_test",
    "single_node_runtime_test",
    "command_log_test",
    "stable_store_test",
    "tcp_transport_test",
    "client_protocol_test",
    "node_config_test",
    "request_fingerprint_test",
    "command_codec_test",
    "session_table_test",
    "rpc_codec_test",
    "raft_state_machine_test",
    "sql_command_preparer_test",
    "raft_node_test",
    "bustub_state_machine_test",
    "catalog_snapshot_test",
    "log_store_test",
    "raft_bustub_cluster_test",
    "snapshot_store_test",
    "read_timestamp_test",
    "distributed_node_test",
)


def discovered_test_binaries(repository_root: Path) -> set[str]:
    sources = []
    for directory in ("recovery", "raft", "distributed"):
        sources.extend((repository_root / "test" / directory).rglob("*_test.cpp"))
    sources.append(repository_root / "test" / "common" / "versioned_frame_test.cpp")
    return {source.stem for source in sources}


def validate_binary_manifest(repository_root: Path) -> None:
    listed = set(TEST_BINARIES)
    discovered = discovered_test_binaries(repository_root)
    missing = sorted(discovered - listed)
    stale = sorted(listed - discovered)
    if missing or stale:
        details = []
        if missing:
            details.append(f"unlisted test sources: {', '.join(missing)}")
        if stale:
            details.append(f"listed targets without a source: {', '.join(stale)}")
        raise RuntimeError("component gate manifest mismatch; " + "; ".join(details))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("build_dir", type=Path)
    parser.add_argument("artifact_dir", type=Path)
    parser.add_argument(
        "--max-attempts",
        type=int,
        choices=(1,),
        default=1,
        help="retained for command-line compatibility; test processes are deliberately never retried",
    )
    parser.add_argument("--timeout-seconds", type=int, default=120)
    return parser.parse_args()


def write_log(path: Path, binary: Path, result: subprocess.CompletedProcess[bytes]) -> None:
    with path.open("ab") as output:
        output.write(f"$ {binary} --gtest_color=no\n".encode())
        output.write(result.stdout)
        output.write(result.stderr)
        output.write(f"\nexit={result.returncode}\n".encode())


def executed_test_count(report_path: Path) -> int:
    """Count concrete RUN test cases instead of trusting a zero process exit."""

    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"missing or invalid GoogleTest JSON report {report_path}: {error}") from error

    def visit(value: object) -> int:
        if isinstance(value, dict):
            if value.get("status") == "RUN" and "classname" in value:
                return 1
            return sum(visit(child) for child in value.values())
        if isinstance(value, list):
            return sum(visit(child) for child in value)
        return 0

    return visit(report)


def main() -> int:
    args = parse_args()
    if args.timeout_seconds < 1:
        print("--timeout-seconds must be positive", file=sys.stderr)
        return 2
    repository_root = Path(__file__).resolve().parents[2]
    try:
        validate_binary_manifest(repository_root)
    except RuntimeError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 2
    test_dir = args.build_dir.resolve() / "test"
    artifact_dir = args.artifact_dir.resolve()
    artifact_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        artifact_dir.mkdir()
    except FileExistsError:
        print(f"artifact directory must be new and empty: {artifact_dir}", file=sys.stderr)
        return 2

    for name in TEST_BINARIES:
        binary = test_dir / name
        if not binary.is_file():
            print(f"missing test binary: {binary}", file=sys.stderr)
            return 2

        log_path = artifact_dir / f"{name}.log"
        report_path = artifact_dir / f"{name}.json"
        command = [str(binary), "--gtest_color=no", f"--gtest_output=json:{report_path}"]
        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=args.timeout_seconds,
            )
        except subprocess.TimeoutExpired as error:
            result = subprocess.CompletedProcess(
                command,
                124,
                stdout=error.stdout or b"",
                stderr=error.stderr or b"",
            )
            write_log(log_path, binary, result)
            sys.stdout.buffer.write(result.stdout)
            sys.stderr.buffer.write(result.stderr)
            print(
                f"FAIL {name}: exceeded {args.timeout_seconds}s timeout; log={log_path}",
                file=sys.stderr,
            )
            return 1
        write_log(log_path, binary, result)
        if result.returncode != 0:
            sys.stdout.buffer.write(result.stdout)
            sys.stderr.buffer.write(result.stderr)
            print(f"FAIL {name}: exit {result.returncode}; log={log_path}", file=sys.stderr)
            return 1

        try:
            executed = executed_test_count(report_path)
        except RuntimeError as error:
            print(f"FAIL {name}: {error}; log={log_path}", file=sys.stderr)
            return 1
        if executed == 0:
            print(f"FAIL {name}: GoogleTest reported zero executed tests; report={report_path}", file=sys.stderr)
            return 1
        print(f"PASS {name} ({executed} tests, one process attempt)")

    print(f"PASS {len(TEST_BINARIES)} component binaries; process_retries=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
