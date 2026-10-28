# Full io_uring comparison repeat — Hana, 2026-10-27

See the [complete comparison](../../uring-readiness/repeat.md) for throughput, paired ranges, prior-cohort comparisons, methodology and limitations. This directory is a new cohort; [the original evidence](../uring-readiness-20261027/readme.md) is preserved unchanged.

- `confirmation/`: 80 uninstrumented echo samples, five interleaved rounds, four backends × two connection counts × two payload sizes.
- `headroom/`: 24 uninstrumented echo samples, three rounds at 64 connections using three client workers on separate physical cores.
- `http-raw/`, `http/`: twelve samples each, three rounds at 64 connections. The raw control uses spawned Tokio tasks; the full Hyper control uses the Tokio runtime root. CPU accounting covers the timed HTTP window.
- `counts-final/`: sixteen separate instrumented samples; counts normalize by total received bytes/payload size, including warmup and boundary echoes. Instrumented rates are excluded from performance conclusions.
- `validated-summary.json`: complete-matrix validation and paired changes. All echo payload-validating clients succeed with empty stderr. HTTP probes succeed, and `wrk` has no socket or status errors. Only teardown peer-reset reports appear in HTTP stderr; two raw-response logs end with interrupted report prefixes.
- `comparison-checks.json`: original/final server hashes, original client hashes, fixed `wrk`, syscall interposer and both runner scripts match the previous cohort. All 680 prior evidence checksums were verified.
- `checkout-checks.json`: the 26 original pending scheduler-file hashes remain unchanged and the fix worktree is clean at `587e74620d1dc1468a588f02b7ea1fd37b722db7`.
- `binary-sha256.txt`: exact executable and `wrk` hashes. The binaries, scheduler patch, build sources and correctness/coverage evidence are retained in the original evidence directory rather than duplicated here. No production code changed for this repeat.
- `run.py`, `http-run.py`, `hello-run.py`: exact orchestration scripts, including the HTTP runner imported by `http-run.py`. Its original layout and CPU pinning are described in the report. `*.log`, `started.txt`, `completed.txt` and `idle.txt` record execution and the idle host check.
- `summarize-followup.py`, `summarize-echo.py`: cohort validators. Revalidate with:

```sh
python3 summarize-followup.py . --validator summarize-echo.py \
  --echo-cohorts confirmation headroom counts-final
```

- `sha256.json`: checksums of every other evidence file.

These are loopback, closed-loop borrowed-buffer workloads. They confirm the reduction in receive syscalls and concurrent throughput improvement, while showing remaining backend differences and sensitivity to client placement.
