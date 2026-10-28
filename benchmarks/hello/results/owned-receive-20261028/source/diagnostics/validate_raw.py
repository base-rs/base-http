#!/usr/bin/env python3
"""Validate ignored input, fixed replies, reuse and EOF in the raw I/O controls."""

import argparse
import hashlib
import json
from pathlib import Path
import select
import socket
import subprocess

MODES = ("base-native-raw", "base-tokio-raw", "tokio-raw", "tokio-task-raw")
RESPONSE = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nHello, world!"


def receive(connection):
    result = bytearray()
    while len(result) < len(RESPONSE):
        chunk = connection.recv(len(RESPONSE) - len(result))
        if not chunk:
            raise RuntimeError("EOF before complete response")
        result.extend(chunk)
    assert result == RESPONSE, result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--backends", nargs="+", default=MODES)
    args = parser.parse_args()
    rows = []
    for mode in args.backends:
        server = subprocess.Popen(
            [str(args.binary.resolve()), mode], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        try:
            assert select.select([server.stdout], [], [], 10)[0], "server readiness timeout"
            address = server.stdout.readline().decode().strip()
            assert address, "server did not report its address"
            host, port = address.rsplit(":", 1)
            with socket.create_connection((host, int(port)), timeout=5) as connection:
                connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                # Each send waits for its response; there is no HTTP parsing here.
                for data in (b"x", b"arbitrary non-HTTP input", b"another message"):
                    connection.sendall(data)
                    receive(connection)
                connection.shutdown(socket.SHUT_WR)
                assert connection.recv(1) == b"", "server did not close after input EOF"
            assert server.poll() is None, "server exited after one connection"
            rows.append({"mode": mode, "fixed_responses": 3, "reuse": True, "peer_half_close": True})
        finally:
            server.terminate()
            _, errors = server.communicate(timeout=10)
            assert not errors, errors.decode(errors="replace")
    args.output.write_text(json.dumps({
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "response_bytes": len(RESPONSE), "read_buffer_bytes": 16384, "results": rows,
    }, indent=2) + "\n")
    print(json.dumps(rows))


if __name__ == "__main__":
    main()
