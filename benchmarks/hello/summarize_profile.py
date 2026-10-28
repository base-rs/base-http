#!/usr/bin/env python3
"""Summarize disjoint exclusive samples on each application's execution thread."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import subprocess


def category(node, ancestors):
    symbol = node["symbol"]
    if re.search(r"\b(?:__)?(?:sendto|sendmsg|writev)(?:\b|_)", symbol):
        return "send syscalls"
    if re.search(r"\b(?:__)?(?:recvfrom|recvmsg|readv|read)(?:\b|_)", symbol):
        return "receive syscalls"
    if "libsystem_malloc" in node["raw"] or re.search(r"\b(?:malloc|calloc|realloc|free)\b", symbol):
        return "allocation/deallocation"
    if any(any(component in frame["symbol"] for component in ("event_listener::", "base_scheduler::cancellation::", "readiness_driver::notification::")) for frame in [*ancestors, node]) or any(word in symbol for word in ("pthread_mutex", "os_unfair_lock")):
        return "cancellation/event/mutex work"
    if any(word in symbol for word in ("kevent", "psynch_cvwait", "__ulock_wait", "mach_msg")):
        return "waiting/readiness syscalls"
    return "remaining user-space code"


def parse(path):
    threads = []
    stack = []
    thread = None
    for line in path.read_text().splitlines():
        if line.startswith("Total number in stack"):
            break
        header = re.match(r"^\s+(\d+) (Thread_.*)", line)
        if header:
            thread = {"name": header[2], "samples": int(header[1]), "nodes": []}
            threads.append(thread)
            stack = []
            continue
        frame = re.match(r"^([ +!:|]*)(\d+) (.*)", line)
        if not frame or thread is None:
            continue
        indent = len(frame[1])
        while stack and stack[-1]["indent"] >= indent:
            stack.pop()
        node = {"indent": indent, "samples": int(frame[2]), "symbol": frame[3].split("  (in ")[0], "raw": frame[3], "children": [], "ancestors": stack.copy()}
        if stack:
            stack[-1]["children"].append(node)
        thread["nodes"].append(node)
        stack.append(node)
    return threads


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path("results/profiles"))
    parser.add_argument("--backends", nargs="+", default=["base-native", "base-tokio", "hyper-tokio"])
    args = parser.parse_args()
    directory = args.directory
    summary = []
    for backend in args.backends:
        raw = directory / f"{backend}.sample.txt"
        demangled = directory / f"{backend}.demangled.txt"
        with raw.open() as source, demangled.open("w") as output:
            subprocess.run(["target/release/profile-symbols"], stdin=source, stdout=output, check=True)
        threads = parse(demangled)
        selected = next(thread for thread in threads if ("base-native-task" if backend.startswith("base-native") or backend == "base-owned" else "main-thread") in thread["name"])
        groups = Counter()
        leaves = Counter()
        for node in selected["nodes"]:
            exclusive = node["samples"] - sum(child["samples"] for child in node["children"])
            assert exclusive >= 0, node["symbol"]
            groups[category(node, node["ancestors"])] += exclusive
            leaves[node["symbol"]] += exclusive
        unattributed = selected["samples"] - sum(groups.values())
        assert unattributed >= 0, (backend, unattributed)
        if unattributed:
            groups["unattributed thread samples"] += unattributed
        row = {
            "backend": backend, "thread": selected["name"], "thread_samples": selected["samples"],
            "categories": {name: {"samples": count, "percent": count / selected["samples"] * 100} for name, count in groups.most_common()},
            "top_exclusive_symbols": leaves.most_common(25),
            "all_thread_counts": [{"name": thread["name"], "samples": thread["samples"]} for thread in threads],
        }
        summary.append(row)
        print(backend, selected["samples"], {name: round(count / selected["samples"] * 100, 1) for name, count in groups.most_common()})
        for symbol, count in leaves.most_common(10):
            print(f"  {count:4} {symbol[:180]}")
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
