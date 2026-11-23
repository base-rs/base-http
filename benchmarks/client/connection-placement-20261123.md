# Client rerun for M6 — 23 November 2026

M6 leaves the client's direct path, `base-open`, where M5 had it, and brings `base-call` level with it: at one connection, `base-call` answers **21.7% more requests per second over HTTP and 17.1% more over HTTPS** than M5's, with 20% and 21% less client CPU per response, since M6 runs `Client::call` on the direct path. At 32 connections the M5 and M6 paths' throughputs are within each other's ranges, and `base-call` uses 9.3% less CPU per response over HTTP. The spawned mode, which passes `Driver::run` a scope so that each connection runs as its own task, is within 2.1% of the scoped mode on every path, which is within the samples' ranges. No path regresses against M5.

This is the M6 rerun the [roadmap](../../design/roadmap.md#m6-version-neutral-connections-and-schedulable-drivers) requires, alongside the [hello benchmark's](../hello/connection-placement.md). It ran on [hana](https://github.com/base-rs/base-benchmark/blob/main/hosts/hana.md), an Intel Xeon E3-1240 v6 with 4 cores and 8 threads, Linux 7.2.14 and Rust 1.99.1, with five repetitions of five seconds after one second of warmup, HTTP and HTTPS, at one and 32 connections, against the same independent Hyper server. The client was pinned to CPU 1 and the server's two Tokio workers to CPUs 2 and 3, one core each, with `--client-cpus 1 --server-cpus 2,3`; the runner held base-benchmark's host lock. `before-open` and `before-call` run the M5 executable, built from base-http main at `9fdc82e` on hana with its own lockfile, and the other Base paths run the M6 candidate, built on base-http#90 at `231671f`; base-http#90 changes only the server, and this benchmark's server is Hyper. The [results](results/m6-20261123/summary.json) keep the summary, the run's [metadata](results/m6-20261123/metadata.json) with the candidate's hash, the source hashes of both executables and the baseline's [metadata](results/m6-20261123/baseline/metadata.json); the samples stay on hana under `~/bench-m6/client-m6-r`. An earlier run on 22 November, with the candidate before base-http#90 and during a ZFS scrub on the host, agreed on every comparison below.

## Throughput and CPU

Rates are responses per second; CPU is the client process's CPU µs per completed response. Each median and range is over five samples.

| Transport | Connections | Client | Median responses/s | Range | Median CPU µs/response |
| --- | ---: | --- | ---: | ---: | ---: |
| HTTP | 1 | `before-open` | 44,502 | 42,716–44,795 | 14.85 |
| HTTP | 1 | `base-open` | 44,589 | 43,465–45,127 | 14.92 |
| HTTP | 1 | `base-open-spawned` | 44,252 | 43,376–45,100 | 14.79 |
| HTTP | 1 | `before-call` | 35,921 | 35,332–37,330 | 18.90 |
| HTTP | 1 | `base-call` | 43,719 | 43,336–45,026 | 14.88 |
| HTTP | 1 | `base-call-spawned` | 43,568 | 43,367–45,030 | 14.84 |
| HTTP | 1 | `hyper` | 41,538 | 40,094–41,643 | 13.84 |
| HTTP | 32 | `before-open` | 91,704 | 87,708–97,492 | 8.83 |
| HTTP | 32 | `base-open` | 93,405 | 89,024–97,013 | 8.66 |
| HTTP | 32 | `base-open-spawned` | 91,940 | 88,001–96,545 | 8.70 |
| HTTP | 32 | `before-call` | 90,230 | 87,520–90,980 | 9.70 |
| HTTP | 32 | `base-call` | 92,352 | 90,697–94,099 | 8.80 |
| HTTP | 32 | `base-call-spawned` | 92,378 | 89,447–95,524 | 8.74 |
| HTTP | 32 | `hyper` | 115,928 | 115,027–116,411 | 7.37 |
| HTTPS | 1 | `before-open` | 34,891 | 34,555–35,844 | 18.83 |
| HTTPS | 1 | `base-open` | 35,658 | 34,976–35,745 | 18.80 |
| HTTPS | 1 | `base-open-spawned` | 35,280 | 35,108–35,515 | 18.76 |
| HTTPS | 1 | `before-call` | 29,361 | 29,338–29,790 | 23.59 |
| HTTPS | 1 | `base-call` | 34,389 | 34,243–35,426 | 18.95 |
| HTTPS | 1 | `base-call-spawned` | 35,097 | 34,703–35,419 | 18.67 |
| HTTPS | 1 | `hyper` | 33,902 | 33,421–34,375 | 16.67 |
| HTTPS | 32 | `before-open` | 69,548 | 67,117–72,013 | 12.04 |
| HTTPS | 32 | `base-open` | 68,440 | 65,677–71,233 | 12.30 |
| HTTPS | 32 | `base-open-spawned` | 68,222 | 66,647–68,771 | 12.26 |
| HTTPS | 32 | `before-call` | 70,095 | 69,101–71,274 | 12.67 |
| HTTPS | 32 | `base-call` | 68,561 | 67,663–69,646 | 12.19 |
| HTTPS | 32 | `base-call-spawned` | 67,903 | 65,447–68,258 | 12.31 |
| HTTPS | 32 | `hyper` | 98,920 | 97,651–99,430 | 8.77 |

## Comparisons

The ratio of medians, for each M6 path against its M5 path and for each spawned path against its scoped one:

| Transport | Connections | Path | Against | Responses/s | CPU µs/response |
| --- | ---: | --- | --- | ---: | ---: |
| HTTP | 1 | `base-open` | `before-open` | +0.2% | +0.5% |
| HTTP | 1 | `base-call` | `before-call` | +21.7% | −21.3% |
| HTTP | 1 | `base-open-spawned` | `base-open` | −0.8% | −0.9% |
| HTTP | 1 | `base-call-spawned` | `base-call` | −0.3% | −0.3% |
| HTTP | 32 | `base-open` | `before-open` | +1.9% | −2.0% |
| HTTP | 32 | `base-call` | `before-call` | +2.4% | −9.3% |
| HTTP | 32 | `base-open-spawned` | `base-open` | −1.6% | +0.5% |
| HTTP | 32 | `base-call-spawned` | `base-call` | +0.0% | −0.6% |
| HTTPS | 1 | `base-open` | `before-open` | +2.2% | −0.1% |
| HTTPS | 1 | `base-call` | `before-call` | +17.1% | −19.7% |
| HTTPS | 1 | `base-open-spawned` | `base-open` | −1.1% | −0.2% |
| HTTPS | 1 | `base-call-spawned` | `base-call` | +2.1% | −1.5% |
| HTTPS | 32 | `base-open` | `before-open` | −1.6% | +2.2% |
| HTTPS | 32 | `base-call` | `before-call` | −2.2% | −3.8% |
| HTTPS | 32 | `base-open-spawned` | `base-open` | −0.3% | −0.3% |
| HTTPS | 32 | `base-call-spawned` | `base-call` | −1.0% | +1.0% |

The spawned mode changes little here because each lane's requests run in the application's task on the direct path, so a connection's own task has little to do per request. Every path ran on one worker; the hello benchmark's server measured what a second worker on an SMT sibling does.

## Reproduce

Build both executables on the measuring host, and archive the M5 executable's sources with its own runner, then run the comparison with the current runner:

```sh
git archive 9fdc82e | tar -x -C bench-m6/m5
(cd bench-m6/m5/benchmarks/client && cargo build --locked --release)
python3 bench-m6/m5/benchmarks/client/run.py --binary bench-m6/m5/benchmarks/client/target/release/base-http-client-benchmark \
  --output bench-m6/m5-sources --toolchain '' --repeats 0
python3 benchmarks/client/run.py --binary benchmarks/client/target/release/base-http-client-benchmark \
  --output client-m6 --toolchain '' --modes reuse --repeats 5 --seconds 5 --warmup 1 --concurrency 1,32 --transports tcp,tls \
  --implementations before-open,base-open,base-open-spawned,before-call,base-call,base-call-spawned,hyper \
  --baseline-binary bench-m6/m5/benchmarks/client/target/release/base-http-client-benchmark \
  --baseline-sources bench-m6/m5-sources/sources.tar.gz --client-cpus 1 --server-cpus 2,3
```
