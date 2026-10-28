# Layer breakdown evidence — Hana, 2026-10-27

The [report](../../layer-breakdown.md) interprets these results. The [current harness](../../layers/readme.md) documents timing boundaries and fresh-source setup.

## Contents and provenance

- `screen/`: 300 timed samples, five repetitions of 60 cases. **Its synthetic Tokio dispatch boundary was mismatched; do not use its `dispatch` or `dispatch-waiting` differences as matched evidence.** Other controls include kernel collection and Tokio's general-list readiness cycle.
- `reader-confirmation/`: 252 samples, seven repetitions of 36 cases, correcting Tokio synthetic dispatch to decode actual collected Mio event snapshots. Includes baseline, reader-inline, Tokio, and standard-library controls.
- `uring-components/`: 170 samples, five repetitions of 34 cases. Final executable with common generic ready-I/O helpers for Base epoll and io_uring, plus future-size metadata. It repeats execution and ready-I/O controls rather than changing production behavior.
- `raw-confirmation/`: 20 raw-server samples, five rounds of baseline epoll, reader-inline epoll, spawned Tokio, and io_uring at 64 connections. Six-second measurements, one-second warmups, independent response validation.
- `operation-counts/`: separate four-second instrumented socket runs for all three backends. Counts accepted sockets' libc receive/send calls and epoll calls; ring operations are outside this interposer's coverage. Fresh sockets cross each interval's boundaries, so small deviations from integer calls per response are expected.
- `task-operation-counts/`: whole-process syscall counts for the final execution controls. Startup and shutdown are included; instrumented rates are not evidence.
- `smoke*/`: successful short assertion runs from the screen, corrected reader and io_uring stages. Build logs also preserve intermediate failed attempts; `build-uring-final.log` is the successful final build.
- `latest-tools/`: current harness, hooks, runner, setup, and candidate builder. `counter-tools/` contains socket/task counting scripts and the C interposer source/shared object. The layer runner's whole-process user/system CPU includes untimed setup and traffic; use Rust's `elapsed_ns` for the stated component boundaries.
- `measurement-sources.tar.gz`: patched disposable scheduler and Tokio dependencies (`deps/`), final probe manifest/source/lockfile (`probe/`), staged HTTP sources/manifests/lockfiles (`sources/`), and initial screen tooling (`tools/`). `base-private-hooks.patch` and `tokio-private-hooks.patch` show the final measurement-only changes. Intermediate probe binaries are preserved, but the final source reflects subsequent additions and refactoring; do not treat final source layout as byte-identical to every earlier build.
- `executables.tar.gz`: seven frozen executables. `probe-screen` is the initial screen; `probe-baseline` and `probe-reader-inline` are the corrected reader comparison; `probe-uring` is the final component suite. `server-baseline` and `server-reader-inline` are the raw-server comparison. `probe` duplicates the original screen binary.
- `scheduler.tar.gz`: unmodified scheduler source at merged main `f05a630db60686526b6145efb284a9c2f61fe8b4`. Package version 0.1.2; this main source includes the OOB PR merged after the registry release.
- `binary-sha256.json`: hashes of all frozen executables. Earlier run metadata records the primary binary hash; `candidate-binary-sha256.json` supplies both reader variants' hashes. The final runner records every binary path's hash.
- `before-source-sha256.json`: original Rust source hashes before installing hooks. `candidate.patch` is the rejected inline-only change; the before/after reader snapshots are included. `packaging-checks.json` confirms sample counts and restored temporary reader. `original-dirty-scheduler-check.json` verifies the 26 previously pending files in the separate original checkout were preserved.
- `sha256.json`: hash index of every other evidence file. Hashes inside `binary-sha256.json` refer to the executable bytes before compression.

## Repeat the final executable comparison

Restore the source and executable archives into `/tmp/base-http-layers-20261027`; measured manifests contain absolute paths to that task root. Restore `run.py` and `benchmark.py` at its root. The raw runner also requires the same wrk build at `wrk/wrk` (wrk 4.2.0, revision `a211dd5a7050b1f9e8a9870b95513060e72ac4a0`). The prior runner's Python source is inside `sources/base-http/benchmarks/hello/run.py`.

```sh
root=/tmp/base-http-layers-20261027

taskset -c 1 python3 "$root/run.py" \
  --binary "$root/binaries/probe-uring" \
  --output "$root/repeat-components" --repetitions 5 \
  --kinds tasks supervisor empty-poll ready-read ready-write

taskset -c 1 python3 "$root/run.py" \
  --binary "$root/binaries/probe-baseline" \
  --compare-candidate "$root/binaries/probe-reader-inline" \
  --output "$root/repeat-reader" --repetitions 7 \
  --kinds readiness dispatch dispatch-waiting events-driver ready-read ready-write ready-short-read \
  --backends base tokio std --widths 1 64

taskset -c 1 python3 "$root/benchmark.py" \
  --variants before:server-baseline:base-native-raw \
             inline:server-reader-inline:base-native-raw \
             tokio:server-baseline:tokio-task-raw \
             iouring:server-baseline:base-iouring-raw \
  --output "$root/repeat-raw" --repetitions 5 --duration 6 --concurrency 64
```

The reader command recreates the original width-1 and width-64 case matrix; see `reader-confirmation/environment.json` for the exact complete specs. Kernel/event and ready-I/O intervals have different state and overlap; their deltas cannot be summed into a request cost.

To rebuild the final probe from the restored source tree:

```sh
CARGO_TARGET_DIR=/tmp/base-http-layer-rebuild \
  cargo build --release --locked --offline \
    --manifest-path /tmp/base-http-layers-20261027/probe/Cargo.toml
```

Cargo's dependency cache must already contain the locked registry packages. The final compiler, CPU, affinity and kernel details are in each cohort's `environment.json`. Rebuilds need not reproduce executable bytes across toolchains. Separate builds, profiling and operation counting from throughput measurements.
