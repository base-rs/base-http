# Scheduler release comparison evidence

See the [report](../../scheduler-release-0.1.1.md) for interpretation.

- `macos/` and `linux/`: all 45 samples, aggregate summaries, environments and source/binary hashes per platform.
- `validation.json`: source/dependency controls, response checks, CPU calibration and original scheduler checkout preservation.
- `before-metadata.json`, `after-metadata.json`: resolved dependency graphs.
- `sources.tar.gz`: both frozen HTTP trees, manifests and lockfiles, plus the comparison runner.
- `binaries.tar.gz`: exact executables measured on both platforms.
- `raw-logs.tar.gz`: all server/wrk logs, smoke validation and build logs. Smoke results are excluded from performance aggregates.
- `sha256.json`: artifact checksums.

To reproduce, extract the sources into a fresh directory and run there. On macOS, install Rust 1.98.1 and supply an absolute path to wrk:

```sh
tar -xzf sources.tar.gz
mkdir -p binaries/macos
for variant in before after; do
  CARGO_TARGET_DIR="$PWD/target-macos-$variant" rustup run 1.98.1 cargo build \
    --manifest-path "$variant/base-http/benchmarks/hello/Cargo.toml" \
    --release --locked --bin base-http-hello-benchmark
  cp "target-macos-$variant/release/base-http-hello-benchmark" "binaries/macos/$variant"
done
rustup run 1.98.1 python3 compare_releases.py --wrk /absolute/path/to/wrk \
  --output results/new-macos --repetitions 5 --duration 8 --concurrency 1 16 64
```

On Linux, use the same frozen trees and Rust version, with `linux` in place of `macos` for binary/build directories. Install Python 3, wrk and util-linux. The runner expects six online CPUs; it pins servers to CPU 0 and `/usr/bin/wrk` to CPUs 2–5. Launch it with `taskset -c 1 python3 compare_releases.py --wrk /usr/bin/wrk --output results/new-linux --repetitions 5 --duration 8 --concurrency 1 16 64`. Run platforms sequentially and keep builds/profilers out of timed intervals.

The standalone runner locates its inputs relative to itself. `sources.tar.gz` includes the required `after/base-http/benchmarks/hello/run.py` and `cpu_usage.py`. Do not remove frozen lockfiles or resolve timer 0.1.1 into the baseline. Output directories must be fresh.
