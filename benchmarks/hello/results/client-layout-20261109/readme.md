# Client layout for the closed-loop scenarios on hana

Screening evidence, gathered 9 November 2026 on [hana](../../../hosts/hana.md): which CPUs base-benchmark-load's workers should run on in `raw-closed` and `http-closed`, with the server on one CPU. It is one host, one day and six rounds, on a host shared with light services, run by the script here rather than by base-benchmark, so it chooses a placement rather than recording a performance result.

## Decision

Run two client workers on two physical cores: `--workers 2` with `--role-cpus client=1,2 --role-cpus server=3` on hana, the layout called C-u below. Of the layouts measured, only two workers on two cores drove each server without the client saturated: each worker was busy for about two-thirds of the window in `raw-closed` and about half in `http-closed`. In `raw-closed` that makes it the only layout that measures the server rather than the client.

- **One worker (A)** is the limit in `raw-closed`: busy for the whole window and below two workers on two cores (C) in every round, by 5–6% at the median (3.5–6.7% in single rounds). In `http-closed` it was busy for 96–98% of the window, flagged as saturated, and within noise of two cores (−0.2% for hyper-tokio, −1.4% for base-native, with a CV of 3.2%).
- **Two workers on one core's SMT siblings (B)** help over one worker in `raw-closed`, +6–7%, but only by saturating both threads: the second thread gives the core at least that much more capacity, just enough to match what two cores drove at two-thirds busy, with no headroom left for a faster server. In `http-closed` B was 1–2% faster than two cores, but with both workers flagged as saturated (85–90% busy) and the server spending 0.7–2.8% less CPU per request. The server's CPU ran at the same frequency, about 4.0 GHz, in both layouts, so turbo does not explain it. The difference is in the server's workload, consistent with a slower client sending requests in larger batches, though this screen did not test that. A layout that changes the server's work is not the one to compare servers with. The siblings' contention also shows in the client: against C, its 99th percentile dispatch lag rose from 0.7–0.8 µs to 1.1–1.3 µs, and its CPUs' softirq time per request from 1.2 µs to 1.8–2.1 µs.
- **Two cores (C, C-u)** had a CV of 0.3–1.4%, except base-native in `http-closed` (C, at 2.4% and 4.0%, from one or two low rounds per cohort); C-u, the form base-benchmark runs, had 1.0% there. B and B-u's CVs were 0.4–2.1%.
- **Pinned or not**: C-u, two workers left to the kernel within CPUs 1 and 2 with the controller unpinned, as `--role-cpus client=1,2` places them, was within 0.6% of C, its workers pinned and its controller on CPU 0, for every backend. B-u was within 0.3% of B by the median of each round's ratio, as "vs C" is computed, except for hyper-tokio in `http-closed`: +2.1%, or +1.6% between the two layouts' medians, where B had a CV of 2.1%.
- **Server CPUs**: the backends are single-threaded, with helper threads that do little. Giving the server its sibling (3,7 against 3, under C) changed nothing measurable (−0.6% for base-native and base-iouring-raw). Under A, two cores (2,3) or two cores and their siblings (2,3,6,7) were +3.2% and +1.8% against CPU 3 by the median of rounds, but rounds ranged from −8.8% to +9.1% with the client saturated, which is inconclusive. One server CPU stays.

The fixed-rate scenarios keep one worker: this screen did not measure them.

## Results

Requests per second are medians of six rounds, with their range and coefficient of variation. "vs C" is the median over rounds of each layout's ratio to layout C in the same round, on server CPU 3. "Busy" is each worker's median busy fraction; the client flags itself as saturated above 0.8. Server CPU is per valid response; client softirq is the time the workers' CPUs spent in software interrupts per valid response, reported only for pinned workers. No sample had an error or an invalid response.

| Layout | Client CPUs | Workers | Controller |
| --- | --- | --- | --- |
| A | 1 | 1, pinned to 1 | Pinned to 0 |
| B | 1 and its sibling 5 | 2, pinned to 1 and 5 | Pinned to 0 |
| C | 1 and 2, two cores | 2, pinned to 1 and 2 | Pinned to 0 |
| B-u | 1 and 5 | 2, unpinned within them | Unpinned within them |
| C-u | 1 and 2 | 2, unpinned within them | Unpinned within them |

### Main campaign

| Scenario | Backend | Layout | Server CPUs | Requests/s | Min–max | CV | vs C | Busy | Saturated | Dispatch lag p99 µs | Server CPU µs/request | Client softirq µs/request |
| --- | --- | --- | --- | ---: | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| raw-closed | base-iouring-raw | A | 3 | 240,355 | 235,525–243,824 | 1.1% | −5.3% | 1.00 | 6/6 | 0.5 | 3.25 | 1.07 |
| raw-closed | base-iouring-raw | B | 3 | 257,585 | 251,364–259,282 | 1.1% | +0.9% | 1.00 1.00 | 6/6 | 1.1 | 3.02 | 1.84 |
| raw-closed | base-iouring-raw | C | 3 | 254,059 | 248,153–255,792 | 1.2% | — | 0.66 0.66 | 0/6 | 0.7 | 3.10 | 1.20 |
| raw-closed | base-iouring-raw | B-u | 3 | 256,619 | 254,327–258,483 | 0.6% | +1.0% | 1.00 1.00 | 6/6 | 1.1 | 3.02 | — |
| raw-closed | base-iouring-raw | C-u | 3 | 255,592 | 252,509–256,978 | 0.6% | +0.5% | 0.67 0.67 | 0/6 | 0.8 | 3.08 | — |
| raw-closed | base-native-raw | A | 3 | 240,353 | 237,362–243,353 | 1.0% | −5.8% | 1.00 | 6/6 | 0.6 | 3.26 | 1.07 |
| raw-closed | base-native-raw | B | 3 | 255,474 | 250,402–257,333 | 1.0% | +0.4% | 0.99 0.99 | 6/6 | 1.1 | 3.06 | 1.87 |
| raw-closed | base-native-raw | C | 3 | 254,649 | 253,775–255,994 | 0.3% | — | 0.67 0.66 | 0/6 | 0.7 | 3.10 | 1.20 |
| raw-closed | base-native-raw | B-u | 3 | 255,694 | 254,568–257,395 | 0.4% | +0.4% | 0.99 0.99 | 6/6 | 1.1 | 3.05 | — |
| raw-closed | base-native-raw | C-u | 3 | 254,649 | 251,312–256,174 | 0.7% | −0.1% | 0.67 0.67 | 0/6 | 0.8 | 3.09 | — |
| http-closed | base-native | A | 3 | 204,909 | 193,714–211,028 | 3.2% | −1.4% | 0.97 | 6/6 | 0.7 | 3.93 | 1.10 |
| http-closed | base-native | B | 3 | 212,486 | 207,326–213,566 | 1.1% | +1.8% | 0.90 0.90 | 6/6 | 1.2 | 3.84 | 1.98 |
| http-closed | base-native | C | 3 | 209,814 | 193,329–213,195 | 4.0% | — | 0.55 0.55 | 0/6 | 0.8 | 3.92 | 1.20 |
| http-closed | base-native | B-u | 3 | 212,123 | 207,972–214,491 | 1.1% | +1.4% | 0.90 0.90 | 6/6 | 1.3 | 3.84 | — |
| http-closed | base-native | C-u | 3 | 209,272 | 206,843–212,167 | 1.0% | +0.6% | 0.55 0.54 | 0/6 | 0.8 | 3.94 | — |
| http-closed | base-native | A | 2,3 | 210,785 | 192,047–215,353 | 4.0% | +0.9% | 0.97 | 6/6 | 0.6 | 3.82 | 1.09 |
| http-closed | base-native | A | 2,3,6,7 | 210,063 | 199,608–212,992 | 2.3% | +0.2% | 0.98 | 6/6 | 0.7 | 3.84 | 1.11 |
| http-closed | hyper-tokio | A | 3 | 194,532 | 186,210–197,992 | 2.1% | −0.2% | 0.96 | 6/6 | 0.6 | 4.19 | 1.11 |
| http-closed | hyper-tokio | B | 3 | 195,779 | 186,891–198,363 | 2.1% | +1.0% | 0.85 0.86 | 6/6 | 1.3 | 4.22 | 2.07 |
| http-closed | hyper-tokio | C | 3 | 194,276 | 188,308–196,461 | 1.4% | — | 0.51 0.51 | 0/6 | 0.8 | 4.25 | 1.20 |
| http-closed | hyper-tokio | B-u | 3 | 198,860 | 196,354–201,082 | 1.0% | +2.1% | 0.85 0.86 | 6/6 | 1.3 | 4.14 | — |
| http-closed | hyper-tokio | C-u | 3 | 194,961 | 191,202–197,046 | 1.3% | −0.4% | 0.50 0.50 | 0/6 | 0.8 | 4.26 | — |

Against base-native's layout A on server CPU 3 in the same round, the server-side check's 2,3 and 2,3,6,7 were +3.2% (rounds −8.8% to +7.6%) and +1.8% (−3.5% to +9.1%).

### Supplementary campaign

Run afterwards, to check whether turbo frequency explains B's lead in `http-closed`, and whether the server gains from its own sibling under C. Each CPU's frequency was sampled from `/proc/cpuinfo` every 250 ms over the window. The median for the server's CPU 3 was 4,000 MHz under B and 3,976–3,997 MHz under C, and the client's workers' CPUs at 3,997–4,000 MHz in both.

| Scenario | Backend | Layout | Server CPUs | Requests/s | Min–max | CV | vs C | Busy | Saturated | Dispatch lag p99 µs | Server CPU µs/request | Client softirq µs/request |
| --- | --- | --- | --- | ---: | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| raw-closed | base-iouring-raw | B | 3 | 256,312 | 252,724–258,220 | 0.8% | +0.5% | 1.00 1.00 | 6/6 | 1.1 | 3.03 | 1.84 |
| raw-closed | base-iouring-raw | C | 3 | 255,256 | 251,561–255,854 | 0.6% | — | 0.66 0.66 | 0/6 | 0.7 | 3.08 | 1.19 |
| raw-closed | base-iouring-raw | C | 3,7 | 253,625 | 248,557–254,037 | 0.9% | −0.6% | 0.66 0.66 | 0/6 | 0.8 | 3.10 | 1.20 |
| http-closed | base-native | B | 3 | 212,147 | 207,876–213,336 | 0.9% | +2.1% | 0.89 0.89 | 6/6 | 1.3 | 3.85 | 1.97 |
| http-closed | base-native | C | 3 | 208,026 | 196,886–210,749 | 2.4% | — | 0.55 0.55 | 0/6 | 0.8 | 3.95 | 1.20 |
| http-closed | base-native | C | 3,7 | 207,755 | 204,992–209,691 | 0.8% | −0.6% | 0.55 0.55 | 0/6 | 0.8 | 3.95 | 1.21 |

## Conditions

- hana as its [host document](../../../hosts/hana.md) describes it on this date: Xeon E3-1240 v6, Linux 7.2.14-arch1-1, the `powersave` governor with `balance_performance` energy preference, turbo enabled. No host setting was changed. It was idle apart from its usual light services; no other benchmark ran, and every sample held the host lock, `/tmp/base-benchmark-host.lock`.
- base-http `eaa13fd`, the hello server built on hana with `cargo build --release --locked` and Rust 1.99.1 (SHA-256 `27c64823d6ff9d64f5343e4ff708e7825393440ee4432ca706fa85e098467615`). base-benchmark-load 0.1.0, `cargo install --locked` on hana (SHA-256 `3e0129da3fa1776d7d8ec714a8aed399fc71706438767e32fb128c7afb9f6adb`).
- The scenarios' measured arguments: closed loop, 64 connections, `--depth 1` for the raw controls, a 2-second warmup, an 8-second window and a 500 ms `--window-lead`, validating the body, `Content-Type` and `Content-Length` of every response. Only the worker and controller placement differed.
- The server was started under `taskset -c SERVER_CPUS`, fresh for every sample, and stopped with `SIGTERM` after it. The client ran under `taskset -c` its CPUs, with `--cpus` and `--controller-cpu 0` in the pinned layouts. The script itself, acting as the runner, ran on CPU 0 with the OS. The controller went on CPU 0 rather than 5, so that it was in the same place in every pinned layout and on neither a worker's CPU nor its sibling; it wakes only to mark the window.
- The script plays base-benchmark's runner for the window markers (`BASE_BENCHMARK_CONTROL=stdio`): at the client's start marker it reads the server's user and system time from `/proc/PID/stat`, every thread included, then acknowledges; it reads them again at the stop marker. So server CPU covers the window and up to 500 ms of the warmup before it, as base-benchmark's does, at the clock tick's 10 ms resolution, and excludes softirq time spent on the server's traffic.
- Six rounds per campaign. Each round ran every configuration once, in an order shuffled by a seeded generator (seeds 20261109 and 20261110), with a 1-second pause between samples.
- Schedule lag applies only at a fixed rate; the closed loop reports dispatch lag, the delay from a worker learning a connection may send to sending.

## Files

- `layout.py`: the campaign driver, which runs each sample as `flock /tmp/base-benchmark-host.lock layout.py sample ...` after checking `pgrep -af` for other benchmarks, and the summaries above. Since the campaigns, a sample also gives up if the server prints no address within 60 seconds, and stops the server and client if it is sent `SIGHUP`, `SIGINT` or `SIGTERM`; what a sample measures is unchanged.
- `samples.csv`: every sample's figures, one row each, from `layout.py table`. The full JSON records stayed on hana and were not kept.

On hana, from a scratch directory holding a clone of base-http at `eaa13fd` and `layout.py`:

```sh
cargo install --locked --root $PWD/load base-benchmark-load --version 0.1.0
(cd base-http/benchmarks/hello && cargo build --release --locked)
SERVER=base-http/benchmarks/hello/target/release/base-http-hello-benchmark
taskset -c 0 python3 -I layout.py campaign --rounds 6 --seed 20261109 \
  --server $SERVER --load load/bin/base-benchmark-load --output samples.jsonl
taskset -c 0 python3 -I layout.py campaign --set supplementary --frequencies --rounds 6 --seed 20261110 \
  --server $SERVER --load load/bin/base-benchmark-load --output supplementary.jsonl
python3 -I layout.py summarize --output samples.jsonl supplementary.jsonl
python3 -I layout.py table --output samples.jsonl supplementary.jsonl > samples.csv
```
