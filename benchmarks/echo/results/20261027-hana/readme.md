# Hana echo evidence — 2026-10-27

See the [results](../../results.md) and [harness instructions](../../readme.md).

- `comparison/`: 80 uninstrumented samples, five repetitions of four backends × two connection counts × two payloads. Four client workers use CPUs 2, 3, 6 and 7 (two physical cores).
- `headroom/`: 24 uninstrumented samples, three repetitions of four backends × two payloads at 64 connections. Three client workers use CPUs 1, 2 and 3 (three physical cores). The controller uses CPU 5.
- `headroom-fixed-workers/`: 12 uninstrumented samples, three repetitions of native epoll and Tokio × two payloads at 64 connections. Four client workers use CPUs 1, 2, 3, 5, 6 and 7 (three physical cores), holding worker count fixed against the initial cohort.
- `operation-counts/`: 16 separate instrumented samples, one repetition of the initial case matrix. Rates from these samples are not throughput evidence. Counts include warmup/setup/cleanup; normalization uses total received payload bytes divided by message size, covering all phases and boundary echoes. The interposer observes accepted-socket libc calls and epoll, not ring submissions/completions.
- `smoke/`, `headroom-smoke/`: short validating runs for both client configurations.
- `tests.log`: six unit tests and thirteen integration tests, including partial writes, zero-progress rejection, client payload corruption rejection and the added native epoll backend.
- `coverage/`: Compressed LLVM region export and summary for the server source. All 425 merged code regions, including 26 regions selected for the extension, execute in the instrumented suite. Coverage uses separate executables from timing.
- `build*.log`, `coverage-build.log`, `coverage-tests.jsonl`: build and test records. No compilation, coverage or syscall instrumentation overlaps a timed throughput cohort.
- `echo.patch`: exact formatted changes to the original scheduler benchmark: native epoll mode, explicit borrowed reader/writer retirement after EOF, usage string, and integration smoke test. No production scheduler implementation changes.
- `benchmark-sources.tar.gz`: the measured echo package plus workspace Cargo manifest/lockfile. Restore these over a fresh extraction of the pinned scheduler archive below.
- `echo.gz`: frozen uninstrumented executable used by both server and client in every cohort. `binary-sha256.json` hashes its uncompressed bytes.
- `tools/`: setup, final flexible runner, coverage helper and cohort/count validator. `run-initial.py` preserves the runner used for the initial cohort; `run-headroom.py` records the version adding client placement options and actual controller affinity.
- `counts.c`, `counts.so`: source and Linux shared object for the separate syscall diagnostics, reused from the layer investigation.
- `reproduction-check.json`: rerunning the corrected setup helper and formatting in a fresh tree exactly reproduces the three measured source changes.
- `source-provenance.json`: pinned source archive hash and revision. `source-checks.json` verifies production Rust source equality to the original archive. `original-checkout-checks.json` verifies the two local scheduler checkouts were preserved.
- `sha256.json`: every other evidence file's hash.

The unmodified scheduler source is [the prior scheduler archive](../../../hello/results/layer-breakdown-20261027/scheduler.tar.gz), from main `f05a630db60686526b6145efb284a9c2f61fe8b4`. Tokio is locked to 1.54.1 and hdrhistogram to 7.5.4. The extension is diagnostic and has not been applied to a production scheduler checkout.

`environment.json` in each cohort captures the actual CPU affinity, binary hash, compiler/kernel and configuration. Raw per-sample client JSON retains validated echo counts and RTT histograms' summary values; every successful client validates every payload. The Python runner's process CPU fields cover complete child lifetimes, including warmup, and cannot be used as timed-window CPU per echo. RTT is closed-loop and does not measure overload latency or server capacity independently of the client.

Use the [harness commands](../../readme.md) to rebuild and repeat. Select a fresh output directory. All throughput cohorts use the same four-second measurement and one-second warmup; their different client configurations must be compared explicitly rather than pooled. Percentage changes against Tokio are medians of within-repetition ratios, not ratios of median rates. The final source and patch allow rebuilding the exact workload; executable bytes may differ across toolchains.
