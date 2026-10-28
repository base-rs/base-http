# Linux epoll and io_uring on Hana

Measured 27 October 2026 on `hana.local`, using released scheduler 0.1.1 as the baseline and unchanged Base HTTP library sources.

The clearest improvement is a fallback socket fix: after `WouldBlock`, await that descriptor's readiness before retrying. At one connection, the revised io_uring mode improves throughput **23.6%** and reduces whole-process CPU per request **40.3%**. At 64 connections, its paired median throughput improvement is **3.7%**. This fix is enabled without an application option.

An independent, opt-in `TCP_INQ` experiment avoids the extra empty receive on Linux. Epoll gains **6.5%** at 64 connections but loses **4.2%** at one connection. This is a workload-dependent option, not a reason to change the default. Hyper remains faster at 64 connections.

## Final confirmation

Five randomized rounds per variant and load, with six-second measurements after one-second warmups: 60 samples. Rates and CPU costs below are medians. Percentage changes are medians of per-round ratios, so they need not equal the ratio of displayed medians. CPU cost includes user and kernel time for the whole server process.

| Connections | Variant | Requests/s | CPU µs/request |
| ---: | --- | ---: | ---: |
| 1 | Epoll 0.1.1 | 58,100 | 9.634 |
| 1 | Epoll + queue hints | 55,669 | 9.514 |
| 1 | io_uring 0.1.1 | 45,281 | 16.746 |
| 1 | io_uring revised, default | 56,402 | 10.013 |
| 1 | io_uring revised + queue hints | 55,794 | 9.432 |
| 1 | Hyper/Tokio | 57,985 | 8.774 |
| 64 | Epoll 0.1.1 | 156,784 | 4.956 |
| 64 | Epoll + queue hints | 166,993 | 4.575 |
| 64 | io_uring 0.1.1 | 146,794 | 5.322 |
| 64 | io_uring revised, default | 152,477 | 5.139 |
| 64 | io_uring revised + queue hints | 157,407 | 4.885 |
| 64 | Hyper/Tokio | 181,822 | 4.184 |

| Connections | Comparison | Throughput change | Observed round range | CPU/request change |
| ---: | --- | ---: | --- | ---: |
| 1 | io_uring default vs 0.1.1 | +23.6% | +22.5% to +25.3% | -40.3% |
| 1 | Epoll hints vs 0.1.1 | -4.2% | -5.7% to -2.8% | -1.4% |
| 1 | io_uring hints vs revised default | -2.1% | -3.9% to +1.6% | -5.8% |
| 1 | io_uring hints vs 0.1.1 | +20.4% | +19.9% to +25.6% | -43.1% |
| 64 | io_uring default vs 0.1.1 | +3.7% | +1.7% to +9.3% | -3.3% |
| 64 | Epoll hints vs 0.1.1 | +6.5% | +0.5% to +11.4% | -7.7% |
| 64 | io_uring hints vs revised default | +5.4% | -7.8% to +7.0% | -7.9% |
| 64 | io_uring hints vs 0.1.1 | +9.9% | -4.2% to +11.0% | -11.1% |

Ranges are observed minimum–maximum changes, not confidence intervals. The io_uring default gain is positive in every confirmation round at both loads. Queue-hint results vary more: one 64-connection io_uring round regresses. Epoll hints are positive in all five 64-connection rounds, but range from approximately zero to 11%. At one connection they regress in every round.

At 64 connections, revised default io_uring remains 15.5% behind Hyper; hinted epoll remains 6.4% behind it, by paired median throughput. The one-connection revised io_uring difference is -3.3%, with individual rounds spanning -5.6% to +2.7%. These results do not establish parity or a win over Hyper.

## Why the fallback fix helps

The io_uring selector delegates listener accept to an independently driven readiness fallback. The HTTP accept future and connection futures share a supervisor task. In `async-io`, `read_with`/`write_with` use an optimistic helper: after the surrounding task is polled again, it retries the operation even when that descriptor's readiness future has not completed. A connection wake therefore causes an unrelated blocked listener to retry `accept4` and rearm the fallback reactor.

`NativeSocket::io` now tries the operation immediately, then awaits `readable()` or `writable()` directly after `WouldBlock`. A successful operation and immediate errors retain their fast path. The independent fallback retains its existing ownership, cancellation, and retirement behavior.

Separate, low-overhead perf syscall counters confirm the removed work:

| One-connection diagnostic | io_uring 0.1.1 | Revised default |
| --- | ---: | ---: |
| `accept4` calls | 303,352 | 4 |
| `epoll_ctl` calls | 303,356 | 16 |
| Receive calls | 304,190 | 400,398 |
| Receives returning `EAGAIN` | 152,048 | 200,198 |
| Sends | 152,140 | 200,198 |

The revised diagnostic completes more requests in its own interval. Receives remain approximately two per response; the improvement comes from removing roughly two accept attempts and two reactor modifications per response in this one-connection case. At higher concurrency, one supervisor wake services many connections, so the avoided listener work was already amortized. This explains the smaller throughput gain there.

These are separate instrumented runs, not the rates used in the confirmation table. Each has a four-second wrk phase plus profiler attachment/exit overhead. Full outputs and perf profiles are preserved.

## TCP queue hints

The baseline Base path makes approximately two receive syscalls per response: one reads the request and one returns `EAGAIN`. The Hyper/Tokio control makes approximately one. Tokio's short-read policy clears readiness, but a short TCP read alone does not prove exhaustion: an urgent-data boundary can leave an ordinary-data suffix queued, even after an alias consumes the urgent byte, without another epoll edge. The regression tests reproduce that case.

`with_tcp_read_hints()` explicitly enables socket-wide `TCP_INQ` for borrowed scalar stream readers. A nonblocking `recvmsg` receives both payload and a remaining-queue hint. Only an intact zero hint proves exhaustion; missing/truncated hints retain readiness. Positive hints preserve queued data and FIN. A generation-aware clear cannot erase a newer notification. The kernel's [TCP_INQ self-test](https://github.com/torvalds/linux/blob/master/tools/testing/selftests/net/tcp_inq.c) exercises the ancillary hint and queued-FIN behavior.

In the 64-connection diagnostics, hinted epoll makes 607,341 receives with 128 returning `EAGAIN`, for 607,210 sends. Hinted io_uring makes 571,600 receives with 45 returning `EAGAIN`, for 571,551 sends. This removes nearly all empty receives. Ancillary processing has its own cost and changes latency, which is why lower-concurrency throughput can worsen despite reduced CPU per request.

The option is disabled by default. Creating an opted-in TCP stream reader tries to enable the socket option; unsupported sockets fall back. Message-oriented receive APIs and external aliases can observe the added ancillary message or control truncation on that socket. Owned payload transfers and vectored/message receives retain their existing paths. The readiness selector's queue-hint optimization requires worker-owned drivers.

## Environment and matched workload

- Physical Intel Xeon E3-1240 v6, four cores/eight threads, 62 GiB RAM; Linux `7.2.11-arch1-1`, x86_64. Hana was idle before the run; ordinary background services remained active. No global sysctl, governor, or other machine setting was changed.
- Rust 1.99.1, commit `58cf8f9c35b355199727627d07ae34a4e2846d67`, Arch LLVM 23.1.3. Identical release profile with `debug = 1` and dependency graph for both binaries. Hyper 1.12.0, Tokio 1.54.1, io-uring 0.7.15, wrk 4.2.0.
- Server and all helper threads pinned to CPU 0; load threads pinned to CPUs 2,3,6,7; controller pinned to CPU 1. CPU 4, the server core's SMT sibling, was unused by this experiment. Shared-host activity and frequency changes remain possible.
- The timed runner uses one wrk thread at one connection and four at 64 connections. Diagnostic profiling uses one or two load threads respectively; its rates are not mixed into the confirmation. Fixed shuffle seed 20261027.
- Full HTTP/1.1 parsing, handler dispatch, fresh response construction and serialization. Static `200 OK`, `Content-Type: text/plain`, 13-byte `Hello, world!` body; Hyper automatic Date disabled. Loopback, TCP_NODELAY, persistent connections, one outstanding request per connection, no TLS or pipelining.
- One application worker and one `FuturesUnordered` supervisor, admission limit 1,024. The Base supervisor runs as one Native task; Hyper uses current-thread Tokio's root. Base uses the existing `serve_connection` API. No HTTP library implementation was changed: all 24 copied Rust library source files match the original files.
- io_uring uses one task/ring worker, 256 entries, transient capacity 1,024, fixed-file capacity 256, and one fallback worker. This HTTP workload uses borrowed buffers: payload receives/sends are synchronous nonblocking syscalls, while the ring supplies readiness. It does **not** measure io_uring-owned payload transfers, multishot receive, registered-buffer throughput, or zero-copy sends.
- `/proc/PID/stat` supplies whole-process user/kernel CPU at 100 Hz. The final sample metadata records one/four load threads, measured duration, CPU cost, RSS, latency and preflight exchanges. p99 values are medians of per-run percentiles, not pooled percentiles; this closed-loop test is not an overload test.
- No builds or profilers ran on Hana during timed samples. Earlier baseline, raw-I/O controls, profiles and three-round screening are retained separately. Different machines/compiler versions were used in earlier macOS/VM reports; do not compare absolute rates across those cohorts.

## Validation and change status

All 60 preflights passed, including status, fields, body, reuse and requested closure: 180 independently checked exchanges. All 120 warmup/timed wrk logs reparse successfully with no socket, timeout or non-success-status errors. Wrk does not check body bytes during timing. Server stderr contains 1,634 complete peer-reset diagnostics and one reset line truncated at shutdown, with no other diagnostic. Peer resets occur when load connections close with a response in flight.

The Linux release suites for Selector, Native and io_uring pass, including their integration, conformance and documentation tests. New regressions cover unrelated socket polls; queue exhaustion after partial/full reads; queued FIN; urgent suffixes including an alias consuming OOB; a newer readiness generation; and io_uring cancellation/close/retirement. The macOS Selector suite also passes under coverage instrumentation, and the targeted fallback test passes without it.

Instrumented Linux suites and Linux Clippy (`--all-targets -- -D warnings`) also pass. The [coverage audit](results/hana-linux-20261027/coverage-audit.json) still finds uncovered regions, including ancillary-message iteration and some readiness/error paths; the repository's complete-region-coverage gate has **not** passed. The optional hint implementation remains a prototype rather than a release-ready change. No `design/` requirement documents exist at the release base. The existing `context/design.md` behavioral contracts were reviewed directly and updated; this review was not independent. Default borrowing, cancellation, generation/lifetime and completion ownership contracts remain intact. The optional socket-wide setting is explicitly documented.

Changes are committed on local scheduler branch `hana-linux-readiness`, in an isolated worktree at `/private/tmp/base-http-hana-epoll-uring-7nxlaica/base-scheduler`:

1. `bd06ccd`: wait for descriptor readiness before retrying fallback socket I/O.
2. `e033123`: opt-in Linux queue hints, their regression tests and documentation.

The original scheduler checkout's 26 pending files were hash-verified unchanged. No PR, merge, release or publication was performed. The fallback fix is the clear default improvement; queue hints should remain an explicitly selected experiment until application-specific measurements justify enabling them.

## PR follow-up: fallback fix only

[base-scheduler PR #60](https://github.com/base-rs/base-scheduler/pull/60) contains only the fallback fix, its regressions, documentation and release note. Independent review confirmed commit `9092b69`; the TCP queue-hint prototype is excluded.

A separate three-round confirmation measured that exact implementation with the hint code absent, using the same Hana setup, one-second warmups and four-second samples at one and 64 connections (18 samples including an interleaved Hyper control). At one connection, released io_uring serves 45,174 req/s and the PR serves 55,565; the paired median change is +22.8%, with CPU/request -39.8%. At 64 connections the medians are 142,848 and 150,243 req/s; paired throughput changes +2.9% and CPU/request -2.9%. Every paired throughput change is positive. These are separate measurements from the five-round tables above.

The exact PR passes full workspace coverage with all targets/features on macOS (29,277/29,277 regions) and Linux (40,587/40,587 regions), plus workspace Clippy with warnings denied on both platforms. Hana's packaged Rust compiler reports standard-library source paths under `/usr/src/debug/rust/`; `LLVM_COV_FLAGS=-ignore-filename-regex=^/usr/src/debug/rust/` excludes only those external compiler files from its coverage export. macOS uses the ordinary gate unchanged. A narrow test-only shim covers reactor-error propagation without invalidating a live descriptor. The queue-hint prototype's earlier coverage limitations remain separate from this completed fallback fix.

The evidence directory adds `pr-confirmation/`, `pr-validation/`, `pr.patch`, `pr-scheduler-source.tar.gz` and the measured `pr-binary.tar.gz`. GitHub's platform/MSRV checks run on the open PR.

## Evidence and reproduction

The [evidence directory](results/hana-linux-20261027/readme.md) contains readable samples, summaries, environments, validation, raw profiles/counters, source snapshots, two patches, a Git bundle and frozen baseline/final binaries. Its artifact SHA256 manifest covers those files.

The exact final source snapshot is preserved. The original baseline launcher was not separately archived before adding hint modes; `baseline-main-reconstructed.rs` removes those modes from the final launcher and is explicitly a reconstruction, not proof of a byte-for-byte rebuild. The baseline scheduler is the exact released Git commit, and the original measured binary is preserved and identified by SHA256. Ordinary default mode in the final binary includes the hint implementation with the option disabled.

After extracting the source snapshots into one root and putting the frozen binaries under `binaries/`, install wrk 4.2.0 under `wrk/wrk` and run:

```sh
taskset -c 1 python3 benchmark.py \
  --variants old-epoll:baseline:base-native epoll-hints:final:base-native-inq \
    old-iouring:baseline:base-iouring iouring-default:final:base-iouring \
    iouring-hints:final:base-iouring-inq hyper:final:hyper-tokio \
  --output results/new-confirmation --repetitions 5 --duration 6 --concurrency 1 64
```

`post-confirmation.sh` reproduces the focused syscall/perf diagnostics. It uses `sudo -n perf` only against server processes started by the script. Select fresh output directories. The runner's CPU affinity assumes Hana's topology; adjust it on another host.
