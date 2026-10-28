# Layered Linux performance controls

This standalone diagnostic separates task execution, readiness bookkeeping, kernel event collection, event publication, and ready socket I/O. The [findings](../layer-breakdown.md) explain which differences also appear in the raw server.

The dependency hooks are private measurement helpers installed into disposable copies. They are never changes to the production scheduler or Tokio packages. This package intentionally cannot build against unmodified registry dependencies.

## Controls and timing boundaries

| Kind | Timed work | Work outside timing |
| --- | --- | --- |
| `tasks` | Spawn, poll and join 1 or 64 identical self-waking futures, approximately one million pending wakes total | Runtime creation and shutdown |
| `supervisor` | A spawned supervisor polls 16 or 64 identical self-waking children in `FuturesUnordered` | Runtime and child collection creation |
| `readiness` | Arm pending reader, publish readiness, poll ready, clear observed generation | Registration creation and initial warmup |
| `readiness-list` | Tokio's general multi-waiter readiness future, same readiness cycle | Registration creation |
| `dispatch` | Decode a prepared event batch and publish readiness without waiters | Kernel collection, generation clearing |
| `dispatch-waiting` | Same event decoding/publication with one pending reader per record | Creating/arming readers, polling their completion, clearing generations |
| `empty-poll` | A full driver turn with zero timeout; Mio also exposes its lower-level collector | Driver creation and closure |
| `events` | Collect/decode queued Unix-socket events from the kernel | Peer writes and socket drains |
| `events-driver` | Collect events and publish to pending readers through the actual driver turn | Peer writes, waiter arming/completion, socket drains |
| `ready-read` | Construct, poll and drop a fresh read future for each ready one-byte receive | Priming readiness, prefill and cleanup; 512 reads per timed batch |
| `ready-write` | Construct, poll and drop a fresh write future for each ready one-byte send | Peer drain and cleanup; 512 writes per timed batch |
| `ready-short-read` | Fresh read future receiving 41 bytes into a 16 KiB buffer | Peer write and readiness preparation; one read per timed interval |

All probes assert progress, byte counts and, where applicable, contents, wake counts, and event checksums. The readiness-cycle Base control supports multiple waiters; Tokio's usual read slot supports one. The separate Tokio list control provides context for that semantic difference. Most measurements are diagnostic operations rather than complete protocol operations. Their time differences overlap and must not be added to estimate request latency.

`base` uses the Native executor and worker-owned epoll. `base-noio` uses the same executor without an attached I/O driver; its unused selector has a background thread. `iouring` attaches the io_uring driver to the Native executor. Tokio uses a current-thread runtime with one spawned supervisor; `tokio-noio` disables its I/O driver. `manual` repeatedly polls the same `FuturesUnordered` without an executor. Ready-I/O controls poll their futures directly with a noop waker to exclude executor work. The `std` control calls nonblocking socket I/O directly.

The borrowed io_uring controls use the existing synchronous nonblocking payload syscalls and readiness fallback. They do not measure submitted owned-buffer receive/send operations or multishot receive performance. Future sizes are recorded as a structural diagnostic; their size alone does not establish execution cost.

## Reproduction

Run on Linux with pinned Tokio 1.54.1, Mio 1.2.4, and the merged scheduler main at `f05a630db60686526b6145efb284a9c2f61fe8b4`. The recorded scheduler archive and patched dependencies are included in the [evidence](../results/layer-breakdown-20261027/readme.md). `setup.py` requires a fresh destination, an extracted scheduler tree, and Tokio 1.54.1 plus the remaining dependencies in Cargo's cache:

```sh
mkdir -p /tmp/base-http-layers/deps/base-scheduler
tar -xzf /path/to/scheduler.tar.gz -C /tmp/base-http-layers/deps/base-scheduler
CARGO_TARGET_DIR=/tmp/base-http-layers-target \
  python3 setup.py /tmp/base-http-layers

taskset -c 1 python3 run.py \
  --binary /tmp/base-http-layers-target/release/base-http-layer-probe \
  --output /tmp/base-http-layers-smoke --smoke

taskset -c 1 python3 run.py \
  --binary /tmp/base-http-layers-target/release/base-http-layer-probe \
  --output /tmp/base-http-layers-measurements --repetitions 5
```

The runner pins every probe thread to CPU 0, so the selected machine must provide CPUs 0 and 1. It interleaves cases with seed 20261027, uses a fresh process for every sample and warmup, and records environment, binary hashes, raw JSON, stderr and summaries. `--kinds`, `--backends` and `--widths` restrict the matrix. One width-64 case uses `total // 64` iterations, so its exact operation count may be slightly below the nominal total. `ns_per_operation` divides the Rust inner timed window by the actual operation count. Recorded whole-process CPU includes untimed setup and traffic and must not be read as CPU for that window.

Compile and profile separately from timing. The original screen predates the corrected Tokio synthetic-event decode boundary; use the seven-round reader confirmation for `dispatch` comparisons. The final io_uring extension also factors ready I/O into a shared generic helper. Compare absolute timings within the same executable cohort.

`build_candidates.py` preserves the rejected reader-inline experiment. It requires the previous staged HTTP sources as its second argument and a shared Cargo target as its third. It builds baseline/candidate probe and raw-server binaries, writes the source patch and hashes, and restores the original reader in `finally`. Its staged HTTP manifests use task-specific dependency paths; the evidence includes the exact measured trees. It is unnecessary for running the layer controls.

To repeat the separate scheduling syscall diagnostic after compiling `counts.c` from the evidence:

```sh
cc -shared -fPIC -O2 -std=gnu11 /path/to/counts.c -ldl -o /tmp/counts.so
taskset -c 1 python3 count_tasks.py \
  --binary /tmp/base-http-layers-target/release/base-http-layer-probe \
  --library /tmp/counts.so --output /tmp/base-http-layers-counts
```

This counts the whole instrumented process, including runtime startup and shutdown. Instrumented throughput is deliberately excluded from the findings.
