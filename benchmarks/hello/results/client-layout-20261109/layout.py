#!/usr/bin/env python3
"""Screen client CPU layouts for the hello benchmark's closed-loop scenarios.

`layout.py campaign` runs every configuration once per round, in a seeded shuffled order, each sample as
`flock /tmp/base-benchmark-host.lock layout.py sample ...`, after checking with `pgrep -af` that no other benchmark is running.
`layout.py sample` starts the server under `taskset`, runs base-benchmark-load under `taskset`, acts as base-benchmark's runner for
the window markers (`BASE_BENCHMARK_CONTROL=stdio`), snapshotting the server's CPU time from /proc at the start and stop markers,
and prints one JSON line. `layout.py summarize` prints the Markdown table, and `layout.py table` the samples as CSV.
"""
import argparse, json, os, random, signal, statistics, subprocess, sys, threading, time

LOCK = "/tmp/base-benchmark-host.lock"
VALIDATION = ["--body", "Hello, world!", "--expect-header", "Content-Type: text/plain", "--expect-header", "Content-Length: 13"]
CONTROLLER = "0"

# Layouts: (client process CPUs, worker arguments). Pinned layouts give each worker a CPU with --cpus and the controller CPU 0;
# unpinned ones (-u) run `--workers 2` within the process's CPUs, as base-benchmark's --role-cpus would place them.
LAYOUTS = {
    "A": ("1," + CONTROLLER, ["--cpus", "1", "--controller-cpu", CONTROLLER]),
    "B": ("1,5," + CONTROLLER, ["--cpus", "1,5", "--controller-cpu", CONTROLLER]),
    "C": ("1,2," + CONTROLLER, ["--cpus", "1,2", "--controller-cpu", CONTROLLER]),
    "B-u": ("1,5", ["--workers", "2"]),
    "C-u": ("1,2", ["--workers", "2"]),
}
SCENARIOS = {
    "raw-closed": (["base-native-raw", "base-iouring-raw"], ["--depth", "1"]),
    "http-closed": (["base-native", "hyper-tokio"], []),
}


def configurations(name):
    if name == "main":
        for scenario, (backends, extra) in SCENARIOS.items():
            for backend in backends:
                for layout in LAYOUTS:
                    yield {"scenario": scenario, "backend": backend, "layout": layout, "server_cpus": "3"}
        for server_cpus in ["2,3", "2,3,6,7"]:
            yield {"scenario": "http-closed", "backend": "base-native", "layout": "A", "server_cpus": server_cpus}
    elif name == "supplementary":
        # Layout C leaves the server core 3 and its sibling 7: does the sibling help the server? And B against C, with frequencies.
        for scenario, backend in [("http-closed", "base-native"), ("raw-closed", "base-iouring-raw")]:
            yield {"scenario": scenario, "backend": backend, "layout": "B", "server_cpus": "3"}
            yield {"scenario": scenario, "backend": backend, "layout": "C", "server_cpus": "3"}
            yield {"scenario": scenario, "backend": backend, "layout": "C", "server_cpus": "3,7"}


def frequencies():
    """Each CPU's current frequency in MHz, as /proc/cpuinfo reports it (from APERF/MPERF on recent kernels)."""
    result, cpu = {}, None
    for line in open("/proc/cpuinfo"):
        if line.startswith("processor"):
            cpu = int(line.split(":")[1])
        elif line.startswith("cpu MHz"):
            result[cpu] = float(line.split(":")[1])
    return result


def process_cpu(pid):
    """User and system ticks of the whole process, and the sum of its threads' scheduler run time in ns."""
    with open(f"/proc/{pid}/stat") as file:
        fields = file.read().rsplit(")", 1)[1].split()
    utime, stime = int(fields[11]), int(fields[12])
    runtime = 0
    threads = 0
    for task in os.listdir(f"/proc/{pid}/task"):
        try:
            with open(f"/proc/{pid}/task/{task}/schedstat") as file:
                runtime += int(file.read().split()[0])
                threads += 1
        except FileNotFoundError:
            pass
    return utime, stime, runtime, threads


def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def sample(arguments):
    configuration = json.loads(arguments.configuration)
    scenario_extra = SCENARIOS[configuration["scenario"]][1]
    client_cpus, worker_arguments = LAYOUTS[configuration["layout"]]
    tick = os.sysconf("SC_CLK_TCK")
    server = subprocess.Popen(["taskset", "-c", configuration["server_cpus"], arguments.server, configuration["backend"]],
                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, start_new_session=True)
    client = None
    try:
        address = server.stdout.readline().strip()
        command = ["taskset", "-c", client_cpus, arguments.load, "closed", "--address", address, "--connections", "64",
                   *worker_arguments, *scenario_extra, *VALIDATION,
                   "--warmup", arguments.warmup, "--duration", arguments.duration, "--window-lead", arguments.window_lead, "--quiet"]
        environment = dict(os.environ, BASE_BENCHMARK_CONTROL="stdio")
        loadavg = open("/proc/loadavg").read().split()[:3]
        client = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                  env=environment, start_new_session=True)
        start = stop_snapshot = None
        record = None
        samples = []
        sampling = threading.Event()

        def sample_frequencies():
            # Sample every 250 ms from the start marker to the stop marker.
            while not sampling.wait(0.25):
                samples.append(frequencies())

        sampler = threading.Thread(target=sample_frequencies)
        for line in client.stdout:
            line = line.rstrip("\n")
            if line == "base-benchmark window start":
                start = (time.monotonic_ns(), process_cpu(server.pid))
                client.stdin.write("base-benchmark window started\n")
                client.stdin.close()
                if arguments.frequencies:
                    sampler.start()
            elif line == "base-benchmark window stop":
                stop_snapshot = (time.monotonic_ns(), process_cpu(server.pid))
                sampling.set()
                if sampler.is_alive():
                    sampler.join()
            elif line.startswith("{"):
                record = json.loads(line)
        stderr = client.stderr.read()
        status = client.wait()
    finally:
        if client is not None and client.poll() is None:
            client.kill()
            client.wait()
        stop(server)
    result = dict(configuration, client_cpus=client_cpus, command=command[3:], status=status, loadavg=loadavg,
                  stderr=stderr[-2000:] if status else "")
    if record is None or start is None or stop_snapshot is None:
        result["failed"] = True
        print(json.dumps(result))
        return
    metrics = {metric["name"]: metric["measurement"]["value"] for metric in record["samples"][0]["metrics"]}
    load = record["extensions"]["base-benchmark-load"]
    valid = metrics["responses_valid"]
    (t0, (u0, s0, r0, _)), (t1, (u1, s1, r1, threads)) = start, stop_snapshot
    server_cpu_ns = (u1 - u0 + s1 - s0) * 1e9 / tick
    result.update(
        valid=valid,
        throughput=valid / (metrics["wall_time"] / 1e9),
        invalid=metrics["responses_invalid"],
        errors=metrics["errors"] + metrics["connect_errors"],
        saturated=load["saturation"]["saturated"],
        saturation_reasons=load["saturation"]["reasons"],
        busy=[worker["busy_fraction"] for worker in load["workers"]],
        worker_cpus=[worker["cpu"] for worker in load["workers"]],
        worker_cpu_ns=[worker["cpu_ns"] for worker in load["workers"]],
        worker_softirq_ns=[worker.get("softirq_ns") for worker in load["workers"]],
        dispatch_lag_p99_ns=metrics["dispatch_lag_p99"],
        service_time_p50_ns=metrics["service_time_p50"],
        service_time_p99_ns=metrics["service_time_p99"],
        client_cpu_ns=metrics["cpu_time"],
        server_window_ns=t1 - t0,
        server_user_ns=(u1 - u0) * 1e9 / tick,
        server_system_ns=(s1 - s0) * 1e9 / tick,
        server_cpu_per_request_ns=server_cpu_ns / valid,
        server_runtime_per_request_ns=(r1 - r0) / valid,
        server_threads=threads,
    )
    if samples:
        result["frequency_mhz"] = {cpu: statistics.median(sample[cpu] for sample in samples) for cpu in samples[0]}
    print(json.dumps(result))


def others(own):
    """Other benchmark processes on the host, as `pgrep -af` reports them."""
    found = subprocess.run(["pgrep", "-af", "base-benchmark|wrk|hello-benchmark"], capture_output=True, text=True).stdout
    return [line for line in found.splitlines() if int(line.split()[0]) not in own]


def campaign(arguments):
    order = list(configurations(arguments.set))
    rng = random.Random(arguments.seed)
    with open(arguments.output, "a") as output:
        for round_index in range(arguments.rounds):
            rng.shuffle(order)
            for position, configuration in enumerate(order):
                while busy := others({os.getpid(), os.getppid()}):
                    print("waiting for:", *busy, sep="\n  ", file=sys.stderr)
                    time.sleep(30)
                command = ["flock", LOCK, sys.executable, "-I", os.path.abspath(__file__), "sample",
                           "--server", arguments.server, "--load", arguments.load, "--warmup", arguments.warmup,
                           "--duration", arguments.duration, "--window-lead", arguments.window_lead,
                           "--configuration", json.dumps(configuration)] + (["--frequencies"] if arguments.frequencies else [])
                line = subprocess.run(command, capture_output=True, text=True, check=True).stdout.strip()
                result = dict(json.loads(line), round=round_index, position=position)
                output.write(json.dumps(result) + "\n")
                output.flush()
                print(round_index, position, configuration["scenario"], configuration["backend"], configuration["layout"],
                      configuration["server_cpus"], f"{result.get('throughput', 0):,.0f}", result.get("busy"),
                      result.get("saturated"), file=sys.stderr)
                time.sleep(arguments.pause)


ORDER = ["A", "B", "C", "B-u", "C-u"]
FIELDS = ["round", "position", "scenario", "backend", "layout", "server_cpus", "client_cpus", "valid", "throughput", "busy",
          "saturated", "dispatch_lag_p99_ns", "server_user_ns", "server_system_ns", "server_cpu_per_request_ns",
          "worker_softirq_ns", "errors", "invalid"]


def load(paths):
    results = []
    for path in paths:
        for line in open(path):
            results.append(dict(json.loads(line), set=os.path.basename(path).split(".")[0]))
    return results


def summarize(arguments):
    """One row per configuration and set: medians over rounds, and the median of each round's ratio to layout C on server CPU 3."""
    rows = {}
    for result in load(arguments.output):
        key = (result["set"], result["scenario"], result["backend"], result["server_cpus"], result["layout"])
        rows.setdefault(key, {})[result["round"]] = result
    print("| Set | Scenario | Backend | Layout | Server CPUs | Requests/s, median | Min–max | CV | vs C | Busy | Saturated | Dispatch lag p99 µs | Server CPU µs/request | Client softirq µs/request | Errors/invalid |")
    print("| --- | --- | --- | --- | --- | ---: | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |")
    for key in sorted(rows, key=lambda key: (key[0], key[1] != "raw-closed", key[2], key[3] != "3", ORDER.index(key[4]))):
        by_round = rows[key]
        good = [r for r in by_round.values() if not r.get("failed")]
        rates = [r["throughput"] for r in good]
        reference = rows.get(key[:3] + ("3", "C"))
        ratios = [r["throughput"] / reference[n]["throughput"] - 1 for n, r in by_round.items() if reference and n in reference]
        versus = "—" if key[3:] == ("3", "C") else f"{statistics.median(ratios) * 100:+.1f}%"
        busy = " ".join(f"{statistics.median(r['busy'][i] for r in good):.2f}" for i in range(len(good[0]["busy"])))
        softirq = [sum(r["worker_softirq_ns"]) / r["valid"] for r in good if None not in r["worker_softirq_ns"]]
        print(f"| {key[0]} | {key[1]} | {key[2]} | {key[4]} | {key[3]} | {statistics.median(rates):,.0f} | {min(rates):,.0f}–{max(rates):,.0f} "
              f"| {statistics.stdev(rates) / statistics.mean(rates) * 100:.1f}% | {versus} | {busy} | {sum(r['saturated'] for r in good)}/{len(good)} "
              f"| {statistics.median(r['dispatch_lag_p99_ns'] for r in good) / 1000:.1f} "
              f"| {statistics.median(r['server_cpu_per_request_ns'] for r in good) / 1000:.2f} "
              f"| {f'{statistics.median(softirq) / 1000:.2f}' if softirq else '—'} "
              f"| {sum(r['errors'] for r in good)}/{sum(r['invalid'] for r in good)} |")


def table(arguments):
    """The samples as CSV, without commands or frequencies."""
    print(",".join(["set"] + FIELDS + ["frequency_mhz_cpu3"]))
    for result in load(arguments.output):
        values = [result["set"]]
        for field in FIELDS:
            value = result.get(field)
            if isinstance(value, list):
                value = " ".join(f"{item:.4f}" if isinstance(item, float) else str(item) for item in value)
            elif isinstance(value, float):
                value = f"{value:.6g}" if field == "busy" else f"{value:.1f}"
            values.append(str(value))
        values.append(str(result.get("frequency_mhz", {}).get("3", "")))
        print(",".join(f'"{value}"' if "," in value else value for value in values))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ["campaign", "sample"]:
        command = commands.add_parser(name)
        command.add_argument("--server", required=True)
        command.add_argument("--load", required=True)
        command.add_argument("--warmup", default="2s")
        command.add_argument("--duration", default="8s")
        command.add_argument("--window-lead", default="500ms")
        command.add_argument("--frequencies", action="store_true", help="sample each CPU's frequency over the window")
    commands.choices["campaign"].add_argument("--rounds", type=int, default=6)
    commands.choices["campaign"].add_argument("--seed", type=int, default=20261109)
    commands.choices["campaign"].add_argument("--pause", type=float, default=1.0)
    commands.choices["campaign"].add_argument("--output", required=True)
    commands.choices["campaign"].add_argument("--set", choices=["main", "supplementary"], default="main")
    commands.choices["sample"].add_argument("--configuration", required=True)
    commands.add_parser("summarize").add_argument("--output", required=True, nargs="+")
    commands.add_parser("table").add_argument("--output", required=True, nargs="+")
    arguments = parser.parse_args()
    {"campaign": campaign, "sample": sample, "summarize": summarize, "table": table}[arguments.command](arguments)


if __name__ == "__main__":
    main()
