# io_uring borrowed stream readiness

This follow-up implements the receive-policy experiment identified by the [echo investigation](../results.md). The scheduler change is local commit `587e746` on `perf/iouring-stream-readiness`, in `../base-scheduler-uring-readiness` relative to the Base HTTP checkout. It is based on main `f05a630db60686526b6145efb284a9c2f61fe8b4`.

The [complete repeat comparison](repeat.md), using the same frozen binaries, confirms the concurrent echo gain (+16.76%/+14.01%), raw-response gain (+16.37%) and full HTTP gain (+15.22%). It retains a separate cohort and records the remaining backend gaps; the original measurements below are unchanged.

## Five-round echo confirmation

Median validated echoes/second; percentage changes are medians of paired within-round ratios:

| Connections | Bytes | Before io_uring | Fixed io_uring | Native epoll | Tokio | Fixed vs before | Fixed vs epoll |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 64 | 79,427 | 78,631 | 79,315 | 78,645 | -1.51% | -1.37% |
| 1 | 4096 | 69,646 | 68,652 | 69,856 | 69,175 | -1.24% | -1.82% |
| 64 | 64 | 202,349 | 237,684 | 240,370 | 250,972 | +16.73% | -0.99% |
| 64 | 4096 | 177,974 | 203,491 | 208,860 | 210,394 | +14.34% | -2.52% |

The unnecessary empty receive explains most of the demonstrated concurrent echo gap. The fix does not establish a general throughput win over epoll or Tokio. At 64 connections it still trails Tokio by 5.16% and 3.88%; the remaining small epoll gap is not fully attributed. At one connection it trades 1.2–1.5% throughput against the old syscall-first policy; compare that case explicitly instead of pooling it with the concurrent improvement.

## Client-placement control

Three client workers on three physical cores give the following paired throughput changes for the fixed io_uring reader:

| Payload | vs before | vs native epoll | vs Tokio |
| ---: | ---: | ---: | ---: |
| 64 bytes | +19.32% | +6.00% | +4.39% |
| 4 KiB | +11.04% | -0.98% | -3.74% |

The large benefit over the old reader survives the placement change. The smaller differences among the fixed reader, epoll and Tokio change with loopback batching and client placement. These cohorts establish removal of the large deficit, without establishing a universal backend winner. The remaining differences do not currently justify extra scheduling or registration machinery.

## Raw response check

The existing raw HTTP response control improves from 201,646 to 231,557 requests/second in median throughput. The paired improvement is +14.83%, with all three pairs improving by 14.14–15.38%. The fixed reader trails native epoll by 1.43% and spawned Tokio by 6.20% in this cohort. This corroborates the echo result using another application loop; it is still a borrowed readiness-driven workload, not an owned receive benchmark.

## Full HTTP check

Full Base HTTP improves from 158,100 to 174,875 requests/second in median throughput. The paired improvement is +9.84%; all three pairs improve, ranging from +5.38% to +14.14%. The paired gap to native epoll is -3.31%, ranging from -10.08% to +1.44%. This short cohort confirms a downstream benefit without establishing full HTTP parity.

The Hyper runtime-root control measures 190,967 requests/second in median throughput; fixed Base io_uring trails it by 6.16% in paired throughput. Its task arrangement differs from Base's spawned supervisor, and this is a separate cohort from the earlier spawned-Hyper comparisons. It does not establish a win over Hyper.

## Final syscall confirmation

Counts use total server payload bytes divided by payload size, including warmup and boundary echoes. Instrumented rates are excluded from throughput evidence.

| 64 connections | Receive calls/echo, 64 bytes | WouldBlock receives/echo, 64 bytes | Receive calls/echo, 4 KiB |
| --- | ---: | ---: | ---: |
| Before io_uring | 2.001277 | 1.000639 | 2.001349 |
| Fixed io_uring | 1.001119 | 0.000560 | 1.001287 |
| Native epoll | 1.001592 | 0.001061 | 1.001857 |
| Tokio | 1.001069 | 0.000534 | 1.001241 |

The extra empty receive is eliminated in every tested connection/payload case. Small setup, EOF and notification-race contributions remain. The interposer does not observe ring submissions or completions.

## Validation and retained work

The final production change has seven added focused regressions covering empty-probe removal, full reads, EOF, racing notifications, cancellation, abandonment, operation errors, selector shutdown and broad/isolated priority hints. Real TCP urgent-data tests exercise opt-in progress with and without an OOB-consuming alias, and switching policy preserves EOF and closed state.

The io_uring suite passes 231 unit tests and 276 shared conformance tests, plus bound-I/O, resource, pool and process suites. The diagnostic echo workload passes six unit and thirteen integration tests. Clippy passes for both packages across all targets with warnings denied; formatting and patch whitespace checks pass.

The shared Bake coverage task measures **7,531/7,531 production source regions**, with three existing invariant-only `unreachable!()` regions excluded automatically. `LLVM_COV_FLAGS` excludes test source files and Hana's unavailable Rust standard-library debug sources; all test executables still run. The unfiltered task could not read `/usr/src/debug/rust/...`, so this is explicitly production coverage, not a claim of complete test-source coverage. Both compile-time OOB modes execute in the suite.

[Evidence](../results/uring-readiness-20261027/readme.md) retains the final 104 uninstrumented echo samples, 24 HTTP samples, 16 final syscall diagnostics, raw logs, frozen binaries, coverage export, sources, checksums and exploratory screen. Every final matrix is complete. Echo stderr is empty; HTTP logs contain only complete peer-reset reports plus three terminated log prefixes, and the load generator reports no socket/status errors. All 525 selected source/manifest files match the committed scheduler fix; HTTP Rust sources are identical between before and after; all 26 pending-file hashes in the original scheduler checkout remain unchanged. The isolated scheduler worktree is clean. The fix is committed locally and has not been published or released.

## Cause and implementation

The borrowed io_uring reader previously retried a synchronous nonblocking receive before consulting its cached ring readiness. After every short request, the next read therefore made an empty receive returning `WouldBlock`, then waited. The validating echo diagnostic observed approximately two receives per echo, versus one for native epoll and Tokio.

The ordinary scalar reader now remembers whether it should probe. It probes initially, continues immediately after a full receive, and clears only its observed readiness generation after a short positive receive. Subsequent reads wait for notification. A newer notification racing the receive survives the clear. Closure flags are sticky and inhibit short-read clearing, preserving the final EOF. No borrowed payload pointer survives a poll, and monitor retirement and worker migration use the existing machinery.

`with_oob_support::<true>()` retains the conservative syscall-first path. Pooled, vectored, flagged and owned completion-based reads retain their existing receive strategies. The public reader interface does not change.

Urgent-data detection needs particular care: Linux's [socket readable callback](https://github.com/torvalds/linux/blob/master/net/core/sock.c) includes priority bits in ordinary TCP wake keys, and [ring poll handling](https://github.com/torvalds/linux/blob/master/io_uring/poll.c) can forward those keys without checking the actual socket poll mask. An ordinary receive produced `POLLIN | POLLPRI` during testing. Treating every such completion as urgent would reject valid traffic. The reader therefore rejects only isolated priority notifications in OOB-disabled mode. As specified by the existing interface, disabled support does not guarantee urgent-data detection or progress across urgent marks; select OOB support when that progress is required, including when a socket alias consumes urgent data.

## Measurement method

Hana, Xeon E3-1240 v6, Linux 7.2.11, Rust 1.99.1. The server uses one worker on CPU 0; its SMT sibling CPU 4 stays unused. The fixed original client executable validates every echoed byte. Before/after server binaries and their source changes are frozen separately.

- Main confirmation: five interleaved repetitions, one-second warmup, four-second measurement, one or 64 connections, 64-byte or 4 KiB messages. Four client workers use CPUs 2, 3, 6 and 7.
- Placement control: three interleaved repetitions at 64 connections; three client workers use physical cores 1, 2 and 3, with the controller on CPU 5.
- HTTP controls: three interleaved repetitions at 64 connections, one-second warmup and three-second measurement, using the existing raw-response control and full Base HTTP hello-world server. The raw Tokio control is spawned; the full Hyper control runs in the Tokio runtime root, as supported by this frozen harness. Those task arrangements differ, so the Hyper comparison is a secondary control. The load generator is the same frozen `wrk` executable for all variants. Scheduler sources come from clean main and the final fix, without private benchmark hooks.
- Syscall counts and `perf` profiles are separate diagnostics. Compilation, coverage, profiling and throughput measurement do not overlap. Echo process CPU percentages include setup/warmup/cleanup and are not CPU per timed echo.

These are loopback, closed-loop measurements with borrowed payload buffers. They do not establish isolated server capacity, physical-network results, owned or multishot payload performance, multi-worker scaling, or a general ordering between io_uring and epoll.

## Reproduce

The [echo harness](../readme.md) explains the common workload and source extension. Apply the retained scheduler patch to the pinned clean archive, restore the same echo extension, build optimized before/after executables and freeze both. Then run:

```sh
taskset -c 1 python3 ../run.py --binary /path/to/after --baseline /path/to/before \
  --output /tmp/uring-confirmation \
  --backends iouring-before iouring-poll native-poll tokio
```

The baseline option fixes the client executable as well as enabling the old server. `profile.py` runs separate record/stat diagnostics against frozen binaries. `summarize.py` validates the complete evidence matrix and computes paired ratios. Each result directory records executable hashes, CPU affinity, configuration and raw output. Percentage changes use medians of within-round ratios, not ratios of the reported throughput medians.
