# Client gap at 32 connections: results

These results support the [client gap report](../../client-gap-20261124.md). They check where Base's HTTP/1 client spends the extra CPU per response that separates it from Hyper's at 32 connections, and they measure one fix: encoding request heads by appending bytes rather than through `core::fmt`.

## Setup

Every run was on [hana](https://github.com/base-rs/base-benchmark/blob/main/hosts/hana.md): an Intel Xeon E3-1240 v6 (4 cores, 8 threads), Linux 7.2.14 and Rust 1.99.1 from the system, with release builds of `benchmarks/client` (`debug = 0`, `--locked`). The client ran on CPU 1 and the independent two-worker Hyper server on CPUs 2 and 3, under base-benchmark's host lock.

The workload is `GET /hello` with an empty body and an 11-byte response, over loopback HTTP or HTTPS with TLS 1.3, at 1 or 32 persistent connections. Every response is validated, and the server checks every request.

Variants:

- **main**: `origin/main` at `df3f438` plus `6392482`, which only makes the benchmark record user and system time and context switches. Its executable has SHA-256 `10cd201d1493cfc7f408d62344eebd0e41f6d777b08fe4478964b5a4a838b383`.
- **candidate** (`head`): `b5df65e`, main plus the change. Its executable has SHA-256 `7c604242fcc6c17f80d1b2f2553ef7315db6d1f9e6321f4c5fca9c56d18c31bb`. The branch's later commits change only the benchmark's types and the records.
- **no probe** (`noprobe`): the candidate plus [`patches/diagnostic-no-probe-on-candidate.patch`](patches/diagnostic-no-probe-on-candidate.patch), run with `DIAG_NO_PROBE=1`. A diagnostic only.
- **Hyper**: main's executable with `--implementation hyper`.

## Files

- [`formal/`](formal/): the interleaved run with the benchmark's own `run.py`, 10 repetitions of 3 seconds after 1 second of warmup, over HTTP and HTTPS at 1 and 32 connections. `before-*` run main's executable. It has the runner's `summary.json`, `metadata.json` and `source-hashes.json`, the baseline's metadata and source hashes in `baseline/`, and `samples.jsonl` with every sample's measured fields, collected by `scripts/samples.py`. `paired.json`, from `scripts/paired.py`, gives each candidate path's change against main's executable in the same repetition.

- [`counters/`](counters/): interleaved `perf stat` runs by `scripts/final-stat.sh`, 10 rounds of 3 seconds per variant, for HTTP at 32 and at 1 connection and for HTTPS at 32 connections. Each `final-<case>.jsonl`, from `scripts/counters.py`, holds one record per sample, normalized by the server's request count, which includes warmup. Each `final-<case>-summary.txt`, from `scripts/perfsum.py`, gives medians and ranges. `perf` slows the one-connection case, whose processes sleep on every response, so its rates are lower than in `formal/`.

- [`diagnostics/`](diagnostics/): earlier, shorter diagnostic runs at 32 connections over HTTP. Each `counters-<run>` file has a `.jsonl` of samples and a `.txt` summary:
  - `stat2` compares main and Hyper, 3 rounds, with cache, TLB and branch counters.
  - `noprobe2` compares main, main with the probe skipped unconditionally, and Hyper, 4 rounds.
  - `yield` runs main's sources with [`patches/diagnostic-switches-on-main.patch`](patches/diagnostic-switches-on-main.patch), 4 rounds. Its variants are `diag` (no switch set), `noprobe` (`DIAG_NO_PROBE=1`), `yield` (`DIAG_YIELD=1`) and `both`, against main and Hyper.
  - `encode` is the first comparison of main and the candidate, 6 rounds.

  `syscalls-strace.txt` and `syscalls-strace-tls.txt` count system calls per response under `strace -f -c` (`scripts/strace.sh`). They are diagnostic: strace slows both clients by different amounts, so only their client-side counts are used. `tcp-segments-nstat.txt` counts TCP segments per response with `nstat` (`scripts/nstat.sh`).

- [`scripts/`](scripts/): the driver scripts exactly as run. `diag.py` runs one server and client pair, optionally wrapped by `perf` or `strace`, and reads the server's context switches from `/proc`. `compare.sh` interleaves variants, `formal.sh` and `final-stat.sh` are the final runs, and `tables.py` produces the report's tables from these files. The rest are as described above, plus `group.py`, which groups profile symbols into components. They assume one checkout per variant under `~/client-gap`.

- [`patches/`](patches/): the diagnostic variants as run.

Profiles were recorded from frame-pointer builds of the same sources, with `perf record -e cycles:u --call-graph fp --user-callchains` and `L1-dcache-load-misses:u`. They are not committed, and their throughputs are not used.

## Outcome

At 32 connections over HTTP, the change saves 949 user instructions per response (20,081 to 19,131) and 8.2% of user cycles. Over HTTPS at 32 connections it saves 956 instructions, and at one connection 949. In the formal run, client CPU per response falls 4.1% by paired median at 32 connections over HTTP, and 4.6% over HTTPS. Throughput rises by +5.2% and +3.5% paired medians, both within the host's run-to-run spread. No metric regresses beyond its range.

The probe's immediate read costs 0.67 µs per response over HTTP and 1.21 µs over HTTPS at 32 connections. Base remains 18.7% behind Hyper in throughput at 32 connections over HTTP, and 30.9% over HTTPS, by paired median.

Not measured: fresh connections, the spawned mode, which this change doesn't touch, and allocation counts.
