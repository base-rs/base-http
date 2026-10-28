#!/usr/bin/env python3
"""Count socket I/O for sequential persistent GETs; this is not a speed test.

Build the macOS interposer first:
  clang -std=c11 -O2 -dynamiclib socket_counts.c -o socket_counts.dylib
"""

import argparse
import hashlib
import json
import mmap
import os
from pathlib import Path
import platform
import re
import selectors
import socket
import struct
import subprocess
import tempfile
import time

OPERATIONS = ("read", "readv", "recv", "recvfrom", "recvmsg", "write", "writev", "send", "sendto", "sendmsg")
METRICS = ("calls", "positive", "would_block", "other_error", "bytes")


def snapshot(memory):
    return struct.unpack("=50Q", memory[:])


def call(connection, request):
    connection.sendall(request)
    response = bytearray()
    while b"\r\n\r\n" not in response:
        chunk = connection.recv(4096)
        if not chunk:
            raise RuntimeError("EOF before response head")
        response.extend(chunk)
    head, body = response.split(b"\r\n\r\n", 1)
    assert head.startswith(b"HTTP/1.1 200 OK\r\n"), head
    fields = dict(line.split(b":", 1) for line in bytes(head).lower().split(b"\r\n")[1:])
    length = int(fields[b"content-length"])
    while len(body) < length:
        chunk = connection.recv(length - len(body))
        if not chunk:
            raise RuntimeError("EOF before body")
        body.extend(chunk)
    assert body == b"Hello, world!", body


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--wrk", type=Path, help="Use the benchmark's t4/c64 workload instead of sequential calls")
    parser.add_argument("--duration", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--modes", nargs="+", default=["base-native", "base-tokio", "hyper-tokio"])
    args = parser.parse_args()
    results = []
    with tempfile.TemporaryDirectory(prefix="base-http-socket-counts-") as temporary:
        for mode in args.modes:
            path = Path(temporary) / mode
            environment = dict(os.environ, DYLD_INSERT_LIBRARIES=str(args.library.resolve()), BASE_HTTP_SOCKET_COUNTS=str(path))
            process = subprocess.Popen([str(args.binary.resolve()), mode], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment)
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    if not selector.select(10):
                        raise RuntimeError("server did not become ready")
                address = process.stdout.readline().decode().strip()
                if not address:
                    raise RuntimeError(process.stderr.read().decode())
                host, port = address.rsplit(":", 1)
                with path.open("r+b") as file, mmap.mmap(file.fileno(), 0) as memory, socket.create_connection((host, int(port)), timeout=5) as connection:
                    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    request = f"GET / HTTP/1.1\r\nHost: {address}\r\n\r\n".encode()
                    for _ in range(100):
                        call(connection, request)
                    time.sleep(0.02)
                    before = snapshot(memory)
                    if args.wrk:
                        load = subprocess.run([str(args.wrk.resolve()), "-t4", "-c64", f"-d{args.duration}s", f"http://{address}/"], check=True, capture_output=True, text=True)
                        requests = int(re.search(r"(\d+) requests in", load.stdout)[1])
                        assert "Socket errors:" not in load.stdout, load.stdout
                        assert "Non-2xx or 3xx responses:" not in load.stdout, load.stdout
                    else:
                        requests = args.requests
                        for _ in range(requests):
                            call(connection, request)
                    time.sleep(0.02)
                    after = snapshot(memory)
                    difference = [right - left for left, right in zip(before, after)]
                    operations = {operation: dict(zip(METRICS, difference[index * 5:index * 5 + 5])) for index, operation in enumerate(OPERATIONS)}
                    if not args.wrk:
                        assert sum(row["positive"] for name, row in operations.items() if name in OPERATIONS[:5]) == requests, operations
                        assert sum(row["positive"] for name, row in operations.items() if name in OPERATIONS[5:]) == requests, operations
                    result = {"mode": mode, "requests": requests, "request_bytes": len(request), "operations": operations}
                    if args.wrk:
                        result["wrk_output"] = load.stdout
                    results.append(result)
            finally:
                process.terminate()
                _, stderr = process.communicate(timeout=10)
                if results and results[-1]["mode"] == mode:
                    results[-1]["server_stderr"] = stderr.decode(errors="replace")
    provenance = {"platform": platform.platform(), "binary": str(args.binary.resolve()), "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(), "library_sha256": hashlib.sha256(args.library.read_bytes()).hexdigest()}
    args.output.write_text(json.dumps({"diagnostic_only": True, "workload": "wrk" if args.wrk else "sequential", "connections": 64 if args.wrk else 1, "pipelining": False, "provenance": provenance, "results": results}, indent=2) + "\n")
    print(f"Saved diagnostic counts: {args.output}")
    for result in results:
        reads = [result["operations"][name] for name in OPERATIONS[:5]]
        writes = [result["operations"][name] for name in OPERATIONS[5:]]
        print(json.dumps({"mode": result["mode"], "requests": result["requests"], "receive_calls": sum(row["calls"] for row in reads), "receive_would_block": sum(row["would_block"] for row in reads), "positive_writes": sum(row["positive"] for row in writes)}))


if __name__ == "__main__":
    main()
