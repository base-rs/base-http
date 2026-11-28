#!/usr/bin/env python3
"""Group `perf report --no-children --sort sym -g none -q` lines into components."""
import re, sys, collections
rules = [
    ("benchmark harness", r"base_http_client_benchmark|hdrhistogram|clock_gettime|futures_unordered|FuturesUnordered|Instant|Hyper as|Base as"),
    ("cancellation", r"cancellation|Cancellation"),
    ("event_listener", r"event_listener"),
    ("pool", r"base_pool|hyper_util::client::legacy::pool|Pooled|Checkout"),
    ("selector/socket I/O", r"base_scheduler_selector|socket2|recv|send|writev|tokio::net|tokio::runtime::io|mio::|Registration|ScheduledIo|TokioIo|poll_read_priv"),
    ("scheduler/tasks", r"base_scheduler_native|async_task|base_scheduler::|tokio::runtime|tokio::sync|atomic_waker|AtomicWaker|waker"),
    ("http1 codec", r"base_http1|base_http::|httparse|hyper::proto|http::header|http::uri|HeaderMap|HeaderName|Authority|Uri"),
    ("client orchestration", r"base_http_client::|hyper::client|hyper_util::client|hyper::body|dispatch"),
    ("hashing", r"Hash|hash"),
    ("bytes", r"bytes::"),
    ("alloc", r"malloc|cfree|free|realloc|alloc::"),
    ("libc other", r"^0x0000"),
    ("fmt/str", r"core::fmt|from_utf8|core::str"),
]
def strip(sym):
    # Keep the outermost path: drop generic arguments, but keep the Self type of `<T as Trait>`.
    if sym.startswith("<"):
        depth, end = 0, 0
        for i, c in enumerate(sym):
            depth += c == "<"
            depth -= c == ">"
            if depth == 0:
                end = i
                break
        inner = sym[1:end]
        sym = inner.split(" as ")[0] + sym[end + 1:]
    out, depth = [], 0
    for c in sym:
        if c == "<":
            depth += 1
        elif c == ">":
            depth -= 1
        elif depth == 0:
            out.append(c)
    return "".join(out)
SHOW = "--show" in sys.argv
for path in [a for a in sys.argv[1:] if a != "--show"]:
    groups = collections.Counter()
    for line in open(path):
        m = re.match(r"\s*([0-9.]+)%\s+\[\.\]\s+(.*)", line)
        if not m:
            continue
        pct, sym = float(m.group(1)), m.group(2)
        sym = strip(sym)
        for name, pattern in rules:
            if re.search(pattern, sym):
                groups[name] += pct
                if SHOW: print(f"   {name[:12]:12s} {pct:5.2f} {sym[:140]}")
                break
        else:
            groups["other"] += pct
    print(f"== {path}")
    for name, pct in groups.most_common():
        print(f"  {name:24s} {pct:6.2f}%")
