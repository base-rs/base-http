# Hana Linux evidence, 27 October 2026

See the [findings and interpretation](../../hana-linux.md). The final confirmation contains 60 interleaved samples, five rounds at each of one and 64 connections for six variants.

- `confirmation/`: final samples, aggregates, environment/binary hashes, independent HTTP preflights and complete warmup/timed wrk/server logs.
- `baseline/`, `raw-before/`, `screen/`, `hints-smoke/`: initial characterization, raw I/O control, preliminary screening and functionality smoke run. These are separate cohorts; smoke rates are not performance evidence.
- `accept-before/`, `accept-after/`: one-connection syscall counters including accept and reactor modifications.
- `counters-before/`, `counters-one-before/`, `hints-counters-after/`: low-overhead syscall counters, with receive-exit events filtered to `ret == -11` (`EAGAIN`).
- `profile-before/`, `profile-one-before/`, `profile-one-after/`, `profile-hints-after/`: readable perf reports. `raw-diagnostics.tar.gz` also preserves original perf data, reports and counter logs. These instrumented rates are excluded from confirmation statistics. The perf commands target only server PIDs started by the scripts.
- `syscalls-before/`: strace summaries. Ptrace substantially changes this workload's scheduling and cannot establish its ordinary receive/EAGAIN frequency.
- `benchmark.py`, `profile.py`, `post-confirmation.sh`: runner and diagnostic scripts. The first two locate their files relative to themselves. The shell script uses the original remote task root; change that root and select fresh output directories when reproducing elsewhere.
- `validation.json`: HTTP/wrk/stderr audit and original-checkout hash checks. `benchmark-metadata.json` records the final resolved dependency graph. `logs/` contains build/test/profile driver output.
- `scheduler-baseline.tar.gz`: exact source at published v0.1.1, commit `3f628896d84a226e53e285fc0f07b7fc25f95531`.
- `scheduler-final.tar.gz`: exact final source, commit `e033123`, on branch `hana-linux-readiness`. `patches/` separates the fallback fix (`bd06ccd`) and optional queue-hint experiment (`e033123`); `scheduler.bundle` preserves the commits and their ancestry.
- `http-and-benchmark-final.tar.gz`: frozen HTTP libraries and final benchmark sources/manifest/lockfile, excluding builds and historical result directories. All 24 library Rust source files match the original workspace. Only the copied benchmark adds Linux modes/local scheduler patches; the ordinary benchmark remains pinned to published 0.1.1.
- `baseline-main-reconstructed.rs`: a functional reconstruction of the initial launcher, removing hint modes from the final one. It was reconstructed after measurement. Do not present it as the original exact launcher or claim a verified byte-for-byte baseline rebuild.
- `binaries.tar.gz`: the actual measured `binaries/baseline` and `binaries/final`, both Linux x86_64. Their SHA256 values were checked against the confirmation environment.
- `state.json`: original source paths, release base and hashes of all 26 original pending scheduler files. `sha256.json` covers the preserved evidence files. Coverage findings are recorded in `coverage-audit.json`.

The local worktree is `/private/tmp/base-http-hana-epoll-uring-7nxlaica/base-scheduler`; its branch is available in the original scheduler repository. No PR, merge, release or publication was performed.

To recreate the final source layout in a fresh directory:

```sh
mkdir base-scheduler
tar -xzf scheduler-final.tar.gz -C base-scheduler
tar -xzf http-and-benchmark-final.tar.gz
tar -xzf binaries.tar.gz
```

Copy the Python runner scripts into that directory, install wrk 4.2.0 under `wrk/wrk`, and use the report's command. The frozen binaries avoid rebuilding. A source build uses the captured benchmark lockfile:

```sh
cargo build --release --locked --manifest-path base-http/benchmarks/hello/Cargo.toml
cp base-http/benchmarks/hello/target/release/base-http-hello-benchmark binaries/final
```

A different compiler or changed source layout may change machine code and performance. Source availability is distinct from verifying an identical binary. CPU affinity in the runner assumes Hana's topology and must be reviewed on other hosts.

## Fallback-only PR follow-up

[PR #60](https://github.com/base-rs/base-scheduler/pull/60), commit `9092b69`, excludes queue hints. `pr-confirmation/` records its exact implementation's separate 18-sample Hana comparison. `pr-validation/` records independent-review provenance, full macOS/Linux coverage-gate and Clippy output, documentation checks and production source/binary hashes. `pr.patch` and `pr-scheduler-source.tar.gz` preserve the change, and `pr-binary.tar.gz` preserves its measured executable. See the report's PR follow-up section for configuration and results. The ordinary five-round experiment and original prototype provenance above remain unchanged.
