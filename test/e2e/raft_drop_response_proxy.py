#!/usr/bin/env python3
"""One-shot external proxy that records and drops a committed client response."""

import argparse
import pathlib
import socket
import struct


CLIENT_MAGIC = b"BCLNT001"
CLIENT_FORMAT_VERSION = 1
MAX_CLIENT_PAYLOAD = 16 * 1024 * 1024


def read_exact(connection: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = connection.recv(size - len(result))
        if not chunk:
            raise RuntimeError("connection closed before a complete frame")
        result.extend(chunk)
    return bytes(result)


def read_frame(connection: socket.socket) -> bytes:
    prefix = read_exact(connection, 16)
    if prefix[:8] != CLIENT_MAGIC or struct.unpack(">I", prefix[8:12])[0] != CLIENT_FORMAT_VERSION:
        raise RuntimeError("invalid client frame prefix")
    payload_size = struct.unpack(">I", prefix[12:16])[0]
    if payload_size > MAX_CLIENT_PAYLOAD:
        raise RuntimeError("client frame exceeds the V1 payload limit")
    return prefix + read_exact(connection, payload_size + 4)


def take(data: bytes, offset: int, size: int) -> tuple[bytes, int]:
    end = offset + size
    if end > len(data):
        raise RuntimeError("truncated client response")
    return data[offset:end], end


def response_payload(frame: bytes, expected_request_id: int) -> bytes:
    payload_size = struct.unpack(">I", frame[12:16])[0]
    payload = frame[16 : 16 + payload_size]
    frame_type = struct.unpack(">I", payload[:4])[0]
    if frame_type != 4:
        raise RuntimeError("upstream frame is not a client response")
    body = payload[4:]
    offset = 0
    request_id_bytes, offset = take(body, offset, 8)
    if struct.unpack(">Q", request_id_bytes)[0] != expected_request_id:
        raise RuntimeError("upstream response request ID does not match the dropped request")
    status_bytes, offset = take(body, offset, 4)
    if struct.unpack(">I", status_bytes)[0] != 1:
        raise RuntimeError("upstream did not commit the response-drop request")
    _, offset = take(body, offset, 8)  # node_id
    _, offset = take(body, offset, 1)  # leader_ready
    has_leader, offset = take(body, offset, 1)
    if has_leader != b"\x00":
        _, offset = take(body, offset, 8)
    address_size_bytes, offset = take(body, offset, 4)
    _, offset = take(body, offset, struct.unpack(">I", address_size_bytes)[0])
    _, offset = take(body, offset, 8 * 5)  # term/commit/applied/published/snapshot base
    has_read_timestamp, offset = take(body, offset, 1)
    if has_read_timestamp != b"\x00":
        _, offset = take(body, offset, 8)
    result_size_bytes, offset = take(body, offset, 4)
    result, offset = take(body, offset, struct.unpack(">I", result_size_bytes)[0])
    if offset != len(body):
        raise RuntimeError("client response has trailing bytes")
    if len(result) != 32:
        raise RuntimeError("upstream committed payload is not a WriteResponseV1")
    version, status, inner_request_id, _, commit_index = struct.unpack(">IIQQQ", result)
    if (
        version != 1
        or status != 1
        or inner_request_id != expected_request_id
        or commit_index == 0
    ):
        raise RuntimeError("upstream WriteResponseV1 does not match the dropped request")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--target-port", type=int, required=True)
    parser.add_argument("--result", type=pathlib.Path, required=True)
    parser.add_argument("--request-id", type=int, required=True)
    arguments = parser.parse_args()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", arguments.listen_port))
        listener.listen(1)
        arguments.result.with_suffix(arguments.result.suffix + ".ready").write_text("ready\n", encoding="ascii")
        client, _ = listener.accept()
        with client, socket.create_connection(("127.0.0.1", arguments.target_port), timeout=10) as upstream:
            upstream.settimeout(10)
            upstream.sendall(read_frame(client))
            encoded_response = response_payload(read_frame(upstream), arguments.request_id)
            arguments.result.write_text(f"response_bytes={encoded_response.hex()}\n", encoding="ascii")
            # Deliberately close without forwarding a byte of the response.


if __name__ == "__main__":
    main()
