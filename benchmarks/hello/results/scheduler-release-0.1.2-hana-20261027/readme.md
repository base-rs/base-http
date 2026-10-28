# Published scheduler 0.1.2 on Hana

Measured 27 October 2026. See the [report](../../scheduler-release-0.1.2.md).

- `confirmation/`: all 75 sample records, summary, environment and 150 warmup/timed wrk logs, with server stderr.
- `graph-verification.json`, `graphs/`: registry-only release cohorts and identical other dependencies/features.
- `validation.json`: HTTP/log audit, frozen binary/source checks and preservation of the scheduler checkout.
- `manifests/`: exact measured benchmark manifests, lockfiles and launcher source for both builds.
- `frozen-builds.tar.gz`: complete measured HTTP library sources and benchmark source for both builds, the two Linux executables, and wrk. Extract into a fresh directory; `sources/` is 0.1.2 and `baseline-sources/` is 0.1.1. Each standalone Cargo package is under `base-http/benchmarks/hello/`. Use `cargo build --release --locked --bin base-http-hello-benchmark` with the recorded compiler.
- `benchmark.py`, `reproduce.sh`: exact Linux runner and timing command. Extract the archive beside these files on Linux. Choose a fresh `--output` path when rerunning, because the existing results directory is preserved. The runner expects CPUs 0–7 with the recorded topology; check affinity before using another host.
- `audit.py`, `verify_graphs.py`: audit scripts using the original task directory layout (`results/confirmation/` for samples). This evidence directory presents that output directly as `confirmation/`.
- `build-logs/`, `before-timing.txt`, `after-timing.txt`, `before/`, `state.json`: resolution/build provenance, idle snapshots, original benchmark files and original pending scheduler hashes.
- `sha256.json`: SHA256 for all other evidence artifacts.

All seven scheduler packages are crates.io 0.1.1 in the baseline and 0.1.2 in the updated build. No experimental TCP queue hints or local scheduler patches are included. No compilation/profiling occurred during timing. Whole-process CPU uses `/proc/PID/stat` at 100 Hz. Both server binaries and all their helper threads run on CPU 0.
