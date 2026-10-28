# Completion-policy evidence — 2026-10-27/28

See the [investigation report](../../ring-overhead/readme.md) and [overhead notes](../../../../overhead.md). The report distinguishes measured throughput, instrumented diagnostics and remaining uncertainty.

The baseline is the short-read-fixed scheduler `587e74620d1dc1468a588f02b7ea1fd37b722db7`, following `f05a630`. The candidate adds deferred task running only for fresh thread-bound workers and raises the io-uring minimum to 0.7.15. No HTTP implementation changes are involved.

## Final samples

- `confirmation/`: 90 uninstrumented echo samples, nine variants, two payloads, five interleaved rounds at 64 connections.
- `single/`: 36 uninstrumented echo samples, six variants, two payloads, three rounds at one connection.
- `http/`, `http-raw/`: separate full and raw HTTP comparisons, 20 samples each, four variants and five interleaved rounds at 64 connections.
- `network/`: direct off-host echo over Wi-Fi, one/four server workers and 64/256 connections. This path leaves the server underutilized and cannot establish maximum server scaling.
- `validated-summary.json`: complete matrix, raw JSON, stderr and HTTP diagnostic checks, followed by paired per-round comparisons. Rates displayed in the report are medians; paired percentages are medians of ratios, not ratios of medians.

Warmup is 1 second and the timed loopback window is 4 seconds. Off-host windows are 3 seconds. The echo client validates every payload, including warmup/boundary requests, and counts only complete round trips inside the common timed window. HTTP checks response semantics before loading and validates wrk socket/status diagnostics. Reset reports at forced HTTP teardown are counted separately, not treated as successful client requests.

## Provenance

- `scheduler.patch`, `scheduler-source.tar.gz`, `scheduler-source-sha256.json`: exact local candidate diff and tracked source contents.
- `expected-scheduler-sha256.json`, `verified-scheduler-sha256.json`: 525 runtime/test/manifest files compared between local candidate and remote build source. Network builds change only the benchmark listener address, retained separately.
- `preservation.json`: all 26 pre-existing pending-file hashes in the original scheduler checkout remain intact; the previous fixed worktree remains clean.
- `initial-review.txt`, `final-review.txt`: independent design/correctness review. The reviewer caught the dependency-minimum requirement; the measured selection was already 0.7.15.
- `binaries/`, `binary-sha256.txt`: frozen measured candidate echo/minimal/HTTP and diagnostic network builds. The original frozen client/server controls are retained in [the previous evidence](../uring-readiness-20261027/readme.md), not duplicated here.
- `dependency-rebuild-sections.json`: dependency-minimum/documentation rebuild preserves every allocated runtime section's bytes, address, flags and file offset, except `.note.gnu.build-id`. The measured candidate binary remains authoritative.
- `minimal-Cargo.lock`, `http-source.tar.gz`, network server source files, and the adjacent harness sources retain workload provenance. The HTTP archive preserves the original remote paths in manifests; recreate that layout or update path dependencies when reproducing.

## Diagnostics and checks

`diagnostics.tar.gz` contains perf data, textual traces/reports, commands, client output and syscall counters. These runs are instrumented and excluded from throughput claims. Trace summaries select the server's ring identities and explicitly exclude unrelated system activity. `trace-owned/` is rejected because send counters and receive CQE bytes differ by one 64-byte message; `trace-owned-repeat/` passes exact byte reconciliation. Other final traces reconcile as well. Retain the rejected diagnostic so the exclusion is auditable.

`candidate-tests.log`, `clippy.log`, `coverage.log`, `coverage-final.json.gz`, and `coverage-export.log` record Linux checks. The shared production-region gate reports 7,543/7,543 measured regions covered, with three invariant-only unreachable regions excluded. The package tests include owned receive/cancel/abandonment/close driven solely by zero-timeout turns and setup fallback cases. Local workspace formatting and diagnostic-source formatting/syntax checks pass.

The existing `design/socket-stream-oob.md` statement requiring conservative io_uring scalar reads conflicts with the inherited short-read fix. Completion scheduling does not change that policy; do not describe the whole inherited requirement document as satisfied by this patch. Cancellation, ownership, fallbacks and nonblocking progress for the changed concern are covered by review and tests.

Hana is an Intel Xeon E3-1240 v6, Linux 7.2.11, Rust 1.99.1/LLVM 23.1.3. The server uses CPU 0, its SMT sibling 4 is unused in loopback, the four client workers use 2,3,6,7 and the controller uses 1. Environment JSON records binaries and topology. Some background SMB activity was present and absolute rates drifted; interleaved comparisons and their full paired ranges are retained. Echo/off-host CPU usage covers whole process lifetimes. HTTP CPU usage covers the timed measurement window.

Final local scheduler commit: `77fe0f8c9082391d7178f720f3f2d6f08c82eaff` on `perf/iouring-deferred-taskrun`. The worktree is clean. No branch was pushed and no PR or release was created.
