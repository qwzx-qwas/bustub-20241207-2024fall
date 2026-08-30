#!/usr/bin/env python3
"""External one-frame Raft proxy with file-controlled loss and snapshot replay."""

import argparse
import pathlib
import signal
import socket
import struct
import threading
import time


RPC_MAGIC = b"BRAFT001"
RPC_FORMAT_VERSION = 1
MAX_RPC_PAYLOAD = 128 * 1024 * 1024


def terminate_on_signal(signum: int, _frame: object) -> None:
    """Preserve the harness's conventional 128+signal process status."""
    raise SystemExit(128 + signum)


def read_exact(connection: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = connection.recv(size - len(result))
        if not chunk:
            raise RuntimeError("truncated Raft frame")
        result.extend(chunk)
    return bytes(result)


def read_frame(connection: socket.socket) -> bytes:
    prefix = read_exact(connection, 16)
    if prefix[:8] != RPC_MAGIC or struct.unpack(">I", prefix[8:12])[0] != RPC_FORMAT_VERSION:
        raise RuntimeError("invalid Raft frame prefix")
    payload_size = struct.unpack(">I", prefix[12:16])[0]
    if payload_size > MAX_RPC_PAYLOAD:
        raise RuntimeError("Raft frame exceeds the V1 payload limit")
    return prefix + read_exact(connection, payload_size + 4)


def take(data: bytes, offset: int, size: int) -> tuple[bytes, int]:
    end = offset + size
    if size < 0 or end > len(data):
        raise RuntimeError("truncated Raft message")
    return data[offset:end], end


def take_u8(data: bytes, offset: int) -> tuple[int, int]:
    value, offset = take(data, offset, 1)
    return value[0], offset


def take_u32(data: bytes, offset: int) -> tuple[int, int]:
    value, offset = take(data, offset, 4)
    return struct.unpack(">I", value)[0], offset


def take_u64(data: bytes, offset: int) -> tuple[int, int]:
    value, offset = take(data, offset, 8)
    return struct.unpack(">Q", value)[0], offset


def take_blob(data: bytes, offset: int) -> tuple[bytes, int]:
    size, offset = take_u32(data, offset)
    return take(data, offset, size)


def message_view(frame: bytes) -> tuple[int, int, str, int, bytes]:
    payload_size = struct.unpack(">I", frame[12:16])[0]
    payload = frame[16 : 16 + payload_size]
    offset = 0
    from_node, offset = take_u64(payload, offset)
    to_node, offset = take_u64(payload, offset)
    group_bytes, offset = take_blob(payload, offset)
    message_type, offset = take_u32(payload, offset)
    message_size, offset = take_u32(payload, offset)
    message, offset = take(payload, offset, message_size)
    if offset != len(payload) or from_node == 0 or to_node == 0 or from_node == to_node:
        raise RuntimeError("invalid Raft envelope")
    try:
        group_id = group_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise RuntimeError("invalid Raft group ID") from error
    return from_node, to_node, group_id, message_type, message


def snapshot_request(message: bytes) -> dict[str, int | str | bytes]:
    offset = 0
    term, offset = take_u64(message, offset)
    leader_id, offset = take_u64(message, offset)
    request_id, offset = take_u64(message, offset)
    snapshot_id_bytes, offset = take_blob(message, offset)
    last_included_index, offset = take_u64(message, offset)
    last_included_term, offset = take_u64(message, offset)
    chunk_offset, offset = take_u64(message, offset)
    total_size, offset = take_u64(message, offset)
    payload_checksum, offset = take_u32(message, offset)
    done, offset = take_u8(message, offset)
    data, offset = take_blob(message, offset)
    if offset != len(message) or done not in (0, 1) or done != int(chunk_offset + len(data) == total_size):
        raise RuntimeError("invalid InstallSnapshot request")
    try:
        snapshot_id = snapshot_id_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise RuntimeError("invalid snapshot ID") from error
    return {
        "term": term,
        "leader_id": leader_id,
        "request_id": request_id,
        "snapshot_id": snapshot_id,
        "last_included_index": last_included_index,
        "last_included_term": last_included_term,
        "offset": chunk_offset,
        "total_size": total_size,
        "payload_checksum": payload_checksum,
        "done": done,
        "data_size": len(data),
        "data": data,
    }


def snapshot_response(message: bytes) -> dict[str, int]:
    offset = 0
    term, offset = take_u64(message, offset)
    request_id, offset = take_u64(message, offset)
    success, offset = take_u8(message, offset)
    stale, offset = take_u8(message, offset)
    complete, offset = take_u8(message, offset)
    match_index, offset = take_u64(message, offset)
    next_offset, offset = take_u64(message, offset)
    if offset != len(message) or success not in (0, 1) or stale not in (0, 1) or complete not in (0, 1):
        raise RuntimeError("invalid InstallSnapshot response")
    return {
        "term": term,
        "request_id": request_id,
        "success": success,
        "stale": stale,
        "complete": complete,
        "match_index": match_index,
        "next_offset": next_offset,
    }


def fields_text(fields: dict[str, int | str]) -> str:
    return " ".join(f"{name}={value}" for name, value in fields.items()) + "\n"


def read_watch(path: pathlib.Path) -> dict[str, int]:
    result: dict[str, int] = {}
    for item in path.read_text(encoding="ascii").split():
        name, separator, value = item.partition("=")
        if not separator:
            raise RuntimeError("invalid snapshot response watch")
        result[name] = int(value)
    return result


class Proxy:
    def __init__(self, target_port: int, controls: pathlib.Path):
        self.target_port = target_port
        self.controls = controls
        self.lock = threading.Lock()
        self.recorded = 0
        self.snapshot_identity: tuple[str, int, int, int, int] | None = None
        self.next_snapshot_offset = 0
        self.recorded_offsets: dict[int, bytes] = {}

    def forward(self, frame: bytes) -> None:
        with socket.create_connection(("127.0.0.1", self.target_port), timeout=2) as target:
            target.sendall(frame)

    def record_snapshot_request(self, frame: bytes, from_node: int, to_node: int, message: bytes) -> None:
        request = snapshot_request(message)
        identity = (
            str(request["snapshot_id"]),
            int(request["last_included_index"]),
            int(request["last_included_term"]),
            int(request["total_size"]),
            int(request["payload_checksum"]),
        )
        with self.lock:
            if (self.controls / "snapshot-recorded").exists():
                return
            if self.snapshot_identity is None:
                if request["offset"] != 0:
                    raise RuntimeError("recorded snapshot transfer did not start at offset zero")
                self.snapshot_identity = identity
            elif identity != self.snapshot_identity:
                raise RuntimeError("snapshot metadata changed during proxy recording")

            chunk_offset = int(request["offset"])
            chunk_data = request.pop("data")
            if not isinstance(chunk_data, bytes):
                raise RuntimeError("snapshot parser did not retain literal chunk bytes")
            if chunk_offset < self.next_snapshot_offset:
                if self.recorded_offsets.get(chunk_offset) != chunk_data:
                    raise RuntimeError("conflicting duplicate snapshot frame")
                return
            if chunk_offset != self.next_snapshot_offset:
                raise RuntimeError("out-of-order snapshot frame observed by proxy")

            frame_path = self.controls / f"snapshot-{self.recorded:08d}.frame"
            metadata = {"from": from_node, "to": to_node, **request}
            frame_path.write_bytes(frame)
            frame_path.with_suffix(".meta").write_text(fields_text(metadata), encoding="utf-8")
            if self.recorded == 0:
                (self.controls / "snapshot-first-request.meta").write_text(fields_text(metadata), encoding="utf-8")
            self.recorded_offsets[chunk_offset] = chunk_data
            self.recorded += 1
            self.next_snapshot_offset += int(request["data_size"])
            if request["done"]:
                if self.next_snapshot_offset != request["total_size"]:
                    raise RuntimeError("completed snapshot recording has a size gap")
                summary = {
                    "chunks": self.recorded,
                    "total_size": int(request["total_size"]),
                    "last_included_index": int(request["last_included_index"]),
                    "last_included_term": int(request["last_included_term"]),
                }
                (self.controls / "snapshot-recorded").write_text(fields_text(summary), encoding="ascii")

    def observe_snapshot_response(self, from_node: int, to_node: int, message: bytes) -> None:
        watch_path = self.controls / "snapshot-response-watch"
        observed_path = self.controls / "snapshot-response-observed"
        if not watch_path.exists() or observed_path.exists():
            return
        response = snapshot_response(message)
        observed = {"from": from_node, "to": to_node, **response}
        watch = read_watch(watch_path)
        if all(observed.get(name) == expected for name, expected in watch.items()):
            observed_path.write_text(fields_text(observed), encoding="ascii")

    def handle(self, connection: socket.socket) -> None:
        frame = read_frame(connection)
        from_node, to_node, _, message_type, message = message_view(frame)
        # "drop" blocks every inbound message to this target. "drop-from-N"
        # blocks only one sender, allowing the other two voters to retain their
        # majority link while an old Leader is isolated bidirectionally.
        if (self.controls / "drop").exists() or (self.controls / f"drop-from-{from_node}").exists():
            return
        self.forward(frame)
        if message_type == 5:
            self.record_snapshot_request(frame, from_node, to_node, message)
        elif message_type == 6:
            self.observe_snapshot_response(from_node, to_node, message)

    def replay_loop(self) -> None:
        while True:
            if (self.controls / "replay").exists() and not (self.controls / "replayed").exists():
                try:
                    frames = sorted(self.controls.glob("snapshot-*.frame"))
                    if not frames or not (self.controls / "snapshot-recorded").exists():
                        raise RuntimeError("snapshot replay requested before a complete transfer was recorded")
                    for frame in frames:
                        self.forward(frame.read_bytes())
                    (self.controls / "replayed").write_text(f"{len(frames)}\n", encoding="ascii")
                except Exception as error:  # pylint: disable=broad-exception-caught
                    (self.controls / "replay-error").write_text(f"{error}\n", encoding="utf-8")
                return
            time.sleep(0.01)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--target-port", type=int, required=True)
    parser.add_argument("--controls", type=pathlib.Path, required=True)
    arguments = parser.parse_args()
    # An ignored disposition survives exec. Reset it explicitly so cleanup is
    # reliable even when the test runner ignores SIGTERM for background jobs.
    signal.signal(signal.SIGTERM, terminate_on_signal)
    arguments.controls.mkdir(parents=True, exist_ok=True)
    proxy = Proxy(arguments.target_port, arguments.controls)
    threading.Thread(target=proxy.replay_loop, daemon=True).start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", arguments.listen_port))
        listener.listen(128)
        (arguments.controls / "ready").write_text("ready\n", encoding="ascii")
        while True:
            connection, _ = listener.accept()
            with connection:
                try:
                    proxy.handle(connection)
                except (ConnectionError, TimeoutError) as error:
                    # A target process may deliberately be stopped or isolated.
                    # Preserve that transport fact for diagnostics, but do not
                    # confuse it with a malformed formal Raft frame.
                    (arguments.controls / "last-transport-error").write_text(f"{error}\n", encoding="utf-8")
                except Exception as error:  # pylint: disable=broad-exception-caught
                    (arguments.controls / "last-error").write_text(f"{error}\n", encoding="utf-8")


if __name__ == "__main__":
    main()
