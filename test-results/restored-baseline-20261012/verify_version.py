#!/usr/bin/env python3
"""Distinguish the two saved builds using real STATUS frames on one TCP stream.

No production changes or test hooks. Runs one disposable node; no quorum or
business workload is needed for STATUS. EOF/reset distinguishes a closed stream;
a timeout or malformed reply is an error, never evidence of the old version.
"""
import hashlib
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / ".cache/final-acceptance/build/bin/bustub-node"
OLD = ROOT / "test-results/storage-fixed-20261011-160420-920SjI"
NEW = ROOT / "test-results/storage-runtime-20261012-124833-DyLmHk"


def manifest(directory):
    return {name: digest for digest, name in
            (line.split("  ", 1) for line in (directory / "SHA256SUMS").read_text().splitlines())}


def crc32c(data):
    value = 0xFFFFFFFF
    for byte in data:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
    return value ^ 0xFFFFFFFF


def exact(connection, size):
    result = bytearray()
    while len(result) < size:
        chunk = connection.recv(size - len(result))
        if not chunk:
            if result:
                raise RuntimeError("truncated reply, not a version result")
            raise EOFError("peer closed the connection")
        result.extend(chunk)
    return bytes(result)


def status(connection, request_id):
    # A real, nonempty V1 STATUS request: type=3 and a nonzero request identity.
    payload = struct.pack(">IQ", 3, request_id)
    frame = b"BCLNT001" + struct.pack(">II", 1, len(payload))
    connection.sendall(frame + payload + struct.pack(">I", crc32c(payload)))
    prefix = exact(connection, 16)
    magic, version, size = struct.unpack(">8sII", prefix)
    if magic != b"BCLNT001" or version != 1 or not 24 <= size <= 16 * 1024 * 1024:
        raise RuntimeError("invalid response prefix")
    try:
        body = exact(connection, size)
        checksum = struct.unpack(">I", exact(connection, 4))[0]
    except (EOFError, ConnectionResetError) as error:
        raise RuntimeError("response was truncated") from error
    if crc32c(body) != checksum:
        raise RuntimeError("response CRC mismatch")
    kind, echoed_id, result, node = struct.unpack(">IQIQ", body[:24])
    if (kind, echoed_id, result, node) != (4, request_id, 2, 1):
        raise RuntimeError(f"STATUS did not succeed: {(kind, echoed_id, result, node)}")


def connection_behavior(environment):
    holders = []
    process = None
    with tempfile.TemporaryDirectory(prefix="bustub-version-probe-") as temporary:
        work = Path(temporary)
        try:
            for _ in range(6):
                item = socket.socket()
                holders.append(item)
                item.bind(("127.0.0.1", 0))
            ports = [item.getsockname()[1] for item in holders]
            # The unused peer ports stay reserved and cannot contact other nodes.
            holders[0].close()
            holders[1].close()
            args = [str(BINARY), "--node-id", "1", "--group-id", "rollback-version-probe",
                    "--data-dir", str(work / "node"), "--buffer-pool-size", "64",
                    "--raft-listen", f"127.0.0.1:{ports[0]}",
                    "--client-listen", f"127.0.0.1:{ports[1]}",
                    "--peer", f"2=127.0.0.1:{ports[2]},127.0.0.1:{ports[3]}",
                    "--peer", f"3=127.0.0.1:{ports[4]},127.0.0.1:{ports[5]}"]
            with (work / "node.log").open("w") as log:
                process = subprocess.Popen(args, stdout=log, stderr=log, env=environment)
                deadline = time.monotonic() + 20
                while "bustub-node started raft=" not in (work / "node.log").read_text():
                    if process.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("node did not start: " + (work / "node.log").read_text())
                    time.sleep(0.02)
                endpoint = ("127.0.0.1", ports[1])
                with socket.create_connection(endpoint, timeout=5) as connection:
                    status(connection, 101)
                    print("request 101 / first connection: OK", flush=True)
                    try:
                        status(connection, 102)
                        persistent = True
                        print("request 102 / SAME connection: OK (new behavior)", flush=True)
                    except (EOFError, BrokenPipeError, ConnectionResetError):
                        persistent = False
                        print("request 102 / SAME connection: CLOSED (old behavior)", flush=True)
                with socket.create_connection(endpoint, timeout=5) as connection:
                    status(connection, 103)
                    print("request 103 / NEW connection: OK (node is still healthy)", flush=True)
                process.terminate()
                if process.wait(timeout=10) != 0:
                    raise RuntimeError("node did not shut down normally")
                return persistent
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for item in holders:
                item.close()


def main():
    old, new = manifest(OLD), manifest(NEW)
    relative = str(BINARY.relative_to(ROOT))
    actual = hashlib.sha256(BINARY.read_bytes()).hexdigest()
    for label, directory in [("OLD", OLD), ("NEW", NEW)]:
        timestamp = ET.parse(directory / "crc.xml").getroot().attrib["timestamp"]
        print(f"{label} historical test timestamp (local +08:00): {timestamp}")
    print("OLD binary SHA256:", old[relative])
    print("NEW binary SHA256:", new[relative])
    print("NOW binary SHA256:", actual)
    count, mismatches = 0, []
    for name, expected in old.items():
        if name.startswith(".cache/"):
            continue
        count += 1
        path = ROOT / name
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            mismatches.append(name)
    print(f"Historical source manifest: {count - len(mismatches)}/{count} match OLD")
    if mismatches:
        print("DIFFERENT FILES:", ", ".join(mismatches))
    environment = dict(os.environ)
    library = str(ROOT / ".cache/toolchains/gcc-16.2.0/lib64")
    environment["LD_LIBRARY_PATH"] = library + (
        ":" + environment["LD_LIBRARY_PATH"] if environment.get("LD_LIBRARY_PATH") else "")
    help_text = subprocess.check_output([str(BINARY), "--help"], env=environment, text=True, timeout=5)
    checkpoint_option = "--local-checkpoint-interval-ms" in help_text
    print("New checkpoint CLI option:", "PRESENT" if checkpoint_option else "ABSENT", flush=True)
    persistent = connection_behavior(environment)
    if actual == old[relative] and not persistent and not checkpoint_option and not mismatches:
        print("RESULT: OLD_VERSION_CONFIRMED")
    elif actual == new[relative] and persistent and checkpoint_option:
        print("RESULT: NEW_VERSION_DETECTED")
    else:
        raise RuntimeError("mixed or unknown version; cannot confirm the rollback")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(f"RESULT: INCONCLUSIVE / FAILED: {error}")
