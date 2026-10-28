# Receive-lifecycle evidence

See [the study](../../receive-lifecycle.md) for the decision and limitations.

The preserved server baseline is scheduler
`5cc179985803e1da812353df73868db668234202`, with the matched per-connection task
controls from the preceding topology study. `production-source.tar.gz` contains
those exact sources; its Cargo patch paths name the preceding experiment root.
`candidate-source.tar.gz` contains the actual build tree with updated paths and
the experimental receive hint. `socket_read.before.rs` and `poll-first.patch`
isolate the only runtime change. No runtime change is retained in the local
scheduler checkout.

- `binaries/`: frozen original diagnostic server, rebuilt timing control,
  poll-first candidate and independent paced client. SHA manifests identify them.
- `build-baseline.log`, `build-poll-first.log`, `build.sh`: builds from the same
  source path and Cargo release settings, with the runtime file changed only
  between builds. `smoke-poll-first.json` validates response bytes, connection
  reuse and peer half-close before timing.
- `timing.sh`, `unpaced/`, `fixed/`: separate randomized uninstrumented samples,
  complete environments, client output and stderr. `http-run.py` and
  `fixed-run.py` are the original harnesses. The former imports `run.py` from the
  archived HTTP sources; update its root paths when reproducing elsewhere.
  The copied unpaced environment incorrectly asserts `controller: [1]`:
  Python orchestration inherits SSH affinity, without explicit pinning. Only the
  paced client's coordinator is explicitly pinned to CPU 1. Server and load
  placement is confirmed by the command lines. Raw metadata remains unchanged.
- `profile.py`, `counts-*`, `profile-*`: initial separate server-context perf
  counts and CPU-clock profiles. Six raw `perf.data` files and their self and
  inclusive reports are retained. Profiles do not measure off-CPU latency.
- `confirm.py`, `confirm-*`: corrected 32-bit return-value probe, but later
  samples overlapped compilation. This cohort is excluded from the clean
  mechanism comparison and all performance conclusions.
- `clean.py`, `clean-*`, `after-timing.sh`: corrected, lighter diagnostic pass
  after all timing and builds. Thread inventory and affinity are recorded for
  each clean run. These instrumented runs are not performance comparisons.
- `probe-definitions.txt`, `format-*.txt`, `probe-return*-format.txt`: actual
  kprobe definitions and tracepoint layouts. The original `s64` return probe is
  invalid for `tcp_recvmsg`'s signed `int` return; its zero EAGAIN counts must not
  be interpreted. The corrected `s32` probe is used in the clean pass.
- `failed-control-setup/`, `perf-probe.stderr`: retained setup failures. Perf's
  FIFO acknowledgment includes a terminal NUL; the working harness strips it.
  Perf here rejects tracepoint alias syntax; repeated submit events are
  distinguished by their ordered command filters.
- `probe-cleanup.log`, `probes-after-cleanup.txt`: removal of the task-specific
  global probes. Other groups are left untouched.
- `after-timing-status.txt`, `after-timing-*-failure.sh`: archival setup failures.
  An unprivileged tracefs glob failed before measurement; its EXIT trap removed
  the probes. After reinstallation, all 12 clean runs completed. A subsequent
  filtered `kprobe_profile` read found no rows and returned exit 1; cleanup again
  succeeded. Kernel probe hit/miss inventory is unavailable. The corrected
  `after-timing.sh` uses explicit event paths and retains an empty inventory
  without treating it as a failed measurement. No zero-missed-probe claim is made.
- `analysis.json`, `analyze.py`: reproducible statistics, paired ranges, raw
  diagnostic ratios and validation. `review.md` records independent review.
- `verify.py`, `verification.log`: exact source-change checks, frozen and timing
  binary hashes, byte-for-byte analysis replay and final probe cleanup checks.

Run `python3 analyze.py` to replay the analysis, then `python3 verify.py` to check
source/binary identity and replay stability. `SHA256SUMS` covers retained
files. Diagnostic ratios divide by all validated responses, including warmup.
Counts also include connection setup, terminal EOF reads and drain; they are
events executing in server tasks, not complete system-wide ring lifecycles.
Events executing in client, interrupt or worker context can be omitted. All
counter running percentages must be 100%; `analyze.py` checks them and event
identity against the saved ordered event list. Perf recording is enabled and
acknowledged before launching the client and disabled after it finishes.

To reproduce timing, extract the candidate source archive at its recorded path
(or revise manifest paths), restore `socket_read.before.rs`, build/copy the
control, apply `poll-first.patch`, then build/copy the candidate. Use fresh output
directories in `timing.sh`; finish builds and profiling before timing. The hint
is tested on Linux 7.2 only and has no older-kernel compatibility fallback.
Do not deploy the diagnostic patch as a portable scheduler change.
