#!/usr/bin/env python3
"""Run one server/client pair with optional wrappers, printing per-request diagnostics.

diag.py BINARY IMPLEMENTATION [--tls] [--concurrency N] [--seconds S] [--warmup W]
        [--client-wrap 'perf stat -x, -e ...'] [--server-wrap '...'] [--out DIR]
"""
import argparse, json, os, selectors, shlex, signal, subprocess, sys, tempfile, time
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("binary")
p.add_argument("implementation")
p.add_argument("--tls", action="store_true")
p.add_argument("--concurrency", type=int, default=32)
p.add_argument("--seconds", type=float, default=5)
p.add_argument("--warmup", type=float, default=1)
p.add_argument("--client-wrap", default="")
p.add_argument("--server-wrap", default="")
p.add_argument("--client-cpus", default="1")
p.add_argument("--server-cpus", default="2,3")
p.add_argument("--client-binary")
p.add_argument("--label", default="")
p.add_argument("--env", action="append", default=[], help="KEY=VALUE for the client")
p.add_argument("--out", type=Path, required=True)
a = p.parse_args()
a.out.mkdir(parents=True, exist_ok=True)

def cpus(text):
    return {int(c) for c in text.split(",") if c}

label = a.label or f'{"tls" if a.tls else "tcp"}-c{a.concurrency}-{a.implementation}-{int(time.time()*1000)}'
identity = a.out / "identity"
if not identity.exists():
    subprocess.run([a.binary, "identity", "--identity", str(identity)], check=True)
suffix = ["--tls"] if a.tls else []
server_cmd = shlex.split(a.server_wrap) + [a.binary, "server", "--identity", str(identity), *suffix]
server = subprocess.Popen(server_cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          preexec_fn=lambda: os.sched_setaffinity(0, cpus(a.server_cpus)), start_new_session=True)
sel = selectors.DefaultSelector(); sel.register(server.stdout, selectors.EVENT_READ)
if not sel.select(timeout=20):
    raise SystemExit("server not ready")
ready = server.stdout.readline().strip()
address = ready.removeprefix("READY ")
client_cmd = shlex.split(a.client_wrap) + [a.client_binary or a.binary, "client", "--implementation", a.implementation,
    "--address", address, "--identity", str(identity), "--concurrency", str(a.concurrency),
    "--seconds", str(a.seconds), "--warmup", str(a.warmup), *suffix]
client_env = dict(os.environ, **dict(e.split("=", 1) for e in a.env))
client = subprocess.run(client_cmd, capture_output=True, text=True, env=client_env,
                        preexec_fn=lambda: os.sched_setaffinity(0, cpus(a.client_cpus)))
# Signal the real server process (the wrapper forwards nothing reliably); signal the whole group.
def switches(pid):
    total = {"voluntary_ctxt_switches": 0, "nonvoluntary_ctxt_switches": 0}
    for task in os.listdir(f"/proc/{pid}/task"):
        for line in open(f"/proc/{pid}/task/{task}/status"):
            key, _, value = line.partition(":")
            if key in total:
                total[key] += int(value)
    return total
server_pid = server.pid
if a.server_wrap:
    server_pid = int(subprocess.run(["pgrep", "-P", str(server.pid)], capture_output=True, text=True).stdout.split()[0])
server_switches = switches(server_pid)
if not a.server_wrap:
    server.send_signal(signal.SIGTERM)
else:
    # Terminate the wrapped benchmark server, the wrapper's child, so the wrapper reports.
    out = subprocess.run(["pgrep", "-P", str(server.pid)], capture_output=True, text=True).stdout.split()
    for pid in out:
        os.kill(int(pid), signal.SIGTERM)
stdout, stderr = server.communicate(timeout=30)
(a.out / f"{label}.client.stdout").write_text(client.stdout)
(a.out / f"{label}.client.stderr").write_text(client.stderr)
(a.out / f"{label}.server.stdout").write_text(stdout)
(a.out / f"{label}.server.stderr").write_text(stderr)
if client.returncode:
    print(client.stderr[-3000:]); raise SystemExit("client failed")
result = json.loads(client.stdout.strip().splitlines()[-1])
stats = [json.loads(l.removeprefix("STATS ")) for l in stdout.splitlines() if l.startswith("STATS ")][0]
result["server"] = stats
result["server_voluntary_switches_per_request"] = server_switches["voluntary_ctxt_switches"] / stats["requests"]
result["server_involuntary_switches_per_request"] = server_switches["nonvoluntary_ctxt_switches"] / stats["requests"]
(a.out / f"{label}.json").write_text(json.dumps(result, indent=2))
print(f'{label}: {result["requests_per_second"]:.0f} req/s, {result["client_cpu_us_per_request"]:.2f} us/req, '
      f'requests={result["requests"]} warm={result["warmup_requests"]} server_requests={stats["requests"]} conns={stats["connections"]}')
