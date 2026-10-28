# Remaining-gap investigation evidence

Hana measurements from 27 October 2026. See the [findings and retained implementation](../../raw-gap-followup.md).

## Contents

- `benchmark/`: six uninstrumented, interleaved cohorts containing all 90 timed samples and their warmups, response validation, environment metadata, binary hashes, server stderr, raw wrk output and paired summaries. Each cohort's `environment.json` records its exact variants, duration and concurrency.
- `diagnostics/`: operation-count diagnostics, external perf statistics, rendered perf reports, fixed-rate load measurements, codec timings, build logs and Linux validation logs. Instrumented throughput is not performance evidence.
- `tools/`: the build, measurement and validation scripts, before/after candidate snapshots and patches, pre-change local sources, codec probe, operation counters and final macOS validation logs.
- `sources/`: final staged HTTP source, integration tests, manifests and lockfiles, including the benchmark. These are Linux staging snapshots; scheduler patch paths refer to the task root on Hana.
- `retained.patch`: the actual seven field/token inline hints and new spawned-Hyper benchmark backend applied to the working sources.
- `binary-sha256.json`: frozen binary hashes, including historical binaries that are not included in this archive.
- `perf-and-binaries.tar.gz`: eight raw perf recordings and five matching Linux executables. Kept compressed to avoid expanding the recordings unnecessarily.
- `sha256.json`: SHA256 of every evidence file except itself.

The cohort sample counts are 20 (`oob-raw-screen`), 15 (`oob-http-context`), 15 (`oob-inline-screen`), 10 (`oob-inline-confirmation`), 15 (`oob-fields-http`) and 15 (`oob-fields-confirmation`). All 90 samples and warmups report zero wrk socket/non-success errors and pass the independent response checks. Raw controls deliberately ignore HTTP framing and are not protocol implementations.

## Provenance and reproduction

The original Hana task root is `/tmp/base-http-linux-gap-vohfc1rh`. Its scheduler source is feature commit `da90905140213e169ea21ad6267f511ffdbbb077`, based on v0.1.2, without any retained follow-up scheduler changes. The [OOB option evidence](../oob-default-20261027/readme.md) preserves that feature's implementation. The original local scheduler checkout's 26 pending-file hashes were checked unchanged.

The archive contains these executables under `binaries/`:

| Executable | Purpose |
| --- | --- |
| `oob-default` | Original frozen server, used for operation counts' unmodified controls and fresh profiles. |
| `raw-followup-before-fields` | Task-context-matched HTTP baseline, including spawned Hyper. |
| `raw-followup-retained` | Retained field candidate, byte-for-byte equal to the measured `raw-followup-fields`. |
| `codec-probe-before-fields` | In-memory baseline. |
| `codec-probe-fields` | In-memory retained candidate. |

The raw perf recordings keep their original `diagnostics/...` paths. Their accompanying text reports are already available without unpacking. Perf symbolization may require restoring the executable to the original recorded path. Diagnostic CAS/direct/inline binaries are not archived; their hashes, source snapshots, patches and build scripts are retained.

`benchmark.py` expects `binaries/`, `wrk/wrk` and `sources/base-http/benchmarks/hello/run.py` under its task root. To reproduce the final confirmation from a restored root containing these inputs:

```sh
taskset -c 1 python3 benchmark.py \
  --variants \
    before:raw-followup-before-fields:base-native \
    fields:raw-followup-retained:base-native \
    hyper-task:raw-followup-before-fields:hyper-task-tokio \
  --output results/fields-recheck \
  --repetitions 5 --duration 10 --concurrency 64
```

The driver pins every server thread to CPU 0 and wrk to CPUs 2, 3, 6 and 7. It uses a fixed shuffle seed and a fresh server per sample. CPU measurements use whole-process `/proc` accounting. The workload, host, compiler and pinned dependencies are described in the report and each environment record. Reproduction on another host requires choosing appropriate CPUs and rebuilding the executables; these scripts preserve the original task paths rather than providing a portable benchmark installation.

`tools/build.py` constructs parity, count, CAS, single-waiter and scheduler-inline variants and restores staged sources afterward. `tools/build_fields.py` constructs the field candidate from pre-change sources and restores them. `tools/build_codec.py` builds the in-memory driver, and `tools/measure.py` performs fresh perf or fixed-rate diagnostics. These scripts expect the original task layout and a shared Cargo target at `/tmp/base-http-hana-epoll-uring-7nxlaica/target`. They are historical experiment builders: do not apply them blindly to the final, already-modified source snapshots. The before files and candidate diffs in `tools/` identify each input precisely.

## Exclusions and final validation

The initial 100,000 requests/second fixed-rate trial missed the required delivery threshold and is excluded; its failure output is preserved in `diagnostics/oob-fixed-stat.log` and `diagnostics/oob-fixed-stat/`. The successful 50,000/second diagnostic is in `oob-fixed-stat-50k/`. The initial codec EOF configuration failed with `IncompleteMessage`; the retained codec source enables Hyper half-close, and only successful complete-response measurements are used.

Final Linux checks are `diagnostics/fields-{base-http,base-http1,base-http-server}-linux-{test,clippy}.log` and `diagnostics/fields-linux-validation.log`. Files ending in `-initial.log` or `-before-integration.log` are superseded: the first harness also misplaced Cargo patch arguments, and the first successful test pass omitted integration-test sources. The complete suites were subsequently staged and rerun. Final totals are 56 + 45 + 28 = 129 tests/documentation examples on Linux, and the same 129 on macOS. Clippy across all targets with `-D warnings` passes on both platforms. Unused Cargo patch notices are retained in the Linux logs.

Final macOS checks are in `tools/*-macos-{test,clippy}.log`; `tools/benchmark-macos-check.log` checks the real benchmark's new backend. Formatting was checked in the actual working checkout. Linux uses Rust 1.99.1 and the staged scheduler 0.1.2 feature; macOS uses Rust 1.85.1 and the existing scheduler development checkout. Final source and binary hashes were checked against the measured field candidate before writing this evidence index.
