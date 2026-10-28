# Native epoll performance experiment

A private worker-owned epoll backend is worthwhile for reducing CPU overhead at low concurrency. Full HTTP used 17.05% less process CPU per request at one connection and 4.18% less at 16 connections in the paired sweep. The 64-connection HTTP case established no repeatable throughput or CPU gain in either cohort. This supports adoption as a CPU-efficiency improvement; it does not establish a substantial high-concurrency HTTP throughput improvement.

## Experiment

The baseline is base-scheduler revision `549603509de7577f76ccce9a8a261be743d07af4`, after the five worker-readiness PRs. It already includes direct registration pointers on Linux, the broadcast notifier, and the immediate successful-write path. The [experimental patch](results/native-epoll-linux-20261027/candidate.patch) changes only the private worker readiness backend and its tests. HTTP sources, compiler, dependencies and release settings are identical between binaries. The independently driven `async-io` services remain in both.

The prototype uses persistent edge-triggered socket registrations, the existing pointer lifetime and retirement rules, a persistent eventfd handled in the dispatch pass, a reusable native event vector, and `epoll_pwait2` timeouts. It removes the general poller's waiter lock, per-turn timerfd setup, unconditional wake-descriptor read/rearm, and preliminary event-count traversal. Its old-kernel fallback rounds finite waits up to milliseconds; compatibility work is listed below.

Measurements ran on an idle Podman AppleHV Linux VM because `ssh hana.local` failed DNS resolution. The VM has six ARM64 vCPUs, 8 GiB memory and Linux `6.12.13-200.fc41.aarch64`. Both binaries were built with Rust 1.98.1 from the same container image and lockfile. The server and all its helper threads were pinned to guest CPU 0, wrk to CPUs 2–5, and the controller to CPU 1. Host physical CPU placement and frequency remain uncontrolled. Absolute rates describe this VM; comparisons use adjacent pairs within each cohort.

The [runner](results/native-epoll-linux-20261027/linux_bench.py) alternates baseline and candidate in randomized adjacent pairs. Each sample starts a fresh server, validates three complete responses and connection reuse, warms up for one second, then measures with wrk. One connection uses one load thread; larger cases use four. Timed traffic uses persistent connections and one outstanding request per connection. Raw mode performs one read and one fixed-response write; full HTTP uses the actual Base HTTP/1 server and handler. No build, profiler or syscall tracer ran during timing.

There are 80 uninstrumented samples and 208,758,124 completed requests, with zero wrk-reported socket, timeout or status errors. Full response validation occurs before each sample; wrk does not validate response bodies during timing. Server stderr contains expected connection-reset messages when wrk closes sockets with an in-flight final request, sometimes truncated by final process termination; those logs are retained with the samples. Linux `/proc/PID/stat` supplies whole-process user/system CPU accounting at 100 ticks/second. CPU/request includes both user and system process time; it excludes unrelated machine, hypervisor and asynchronous kernel work.

## Paired sweep

Five pairs per condition, eight measured seconds per sample. Before/after rates below are separate medians. Changes are medians of the five matched ratios, so they need not equal the ratio of the displayed rate medians. Negative CPU change means less CPU per completed request.

| Workload | Connections | Before req/s | After req/s | Paired throughput change | Paired CPU/request change |
| --- | ---: | ---: | ---: | ---: | ---: |
| raw | 1 | 29,885 | 30,269 | +1.16% | -24.76% |
| raw | 16 | 362,240 | 367,812 | +1.55% | -5.45% |
| raw | 64 | 396,224 | 403,479 | +1.62% | -1.60% |
| http | 1 | 29,076 | 29,177 | +0.37% | -17.05% |
| http | 16 | 276,134 | 286,597 | +2.66% | -4.18% |
| http | 64 | 317,439 | 319,766 | -0.27% | -0.93% |

Full HTTP median CPU cost was 9.427 → 7.746 µs/request at one connection, 2.330 → 2.236 at 16, and 2.256 → 2.235 at 64. Every pair reduced CPU cost at one and 16 connections. At 64, the paired CPU range was −2.94% to +2.69%, and the throughput range was −7.63% to +6.13%. One-connection HTTP throughput also varied in sign; its clear improvement is CPU efficiency.

## Longer confirmation

Five further pairs at 64 connections, 12 measured seconds per sample:

| Workload | Connections | Before req/s | After req/s | Paired throughput change | Paired CPU/request change |
| --- | ---: | ---: | ---: | ---: | ---: |
| raw | 64 | 421,111 | 430,931 | +0.93% | -1.54% |
| http | 64 | 348,614 | 352,038 | +0.45% | -0.11% |

Raw CPU/request improved in all five pairs in both cohorts, with median paired reductions of 1.60% and 1.54%. Full HTTP did not establish a meaningful improvement: confirmation paired throughput ranged from −0.17% to +2.43%, and CPU/request from −2.60% to +0.05%, with median CPU change −0.11%. Full HTTP p99 was 0.271 → 0.284 ms in the sweep and 0.270 → 0.269 ms in confirmation, so these measurements establish no stable tail-latency improvement either.

Absolute rates increased between cohorts; this is another reason to use adjacent paired comparisons and avoid pooling absolute rates from the two runs. The server process used less than one full core in these tests, so these rates do not establish its CPU-saturated capacity. The workload establishes neither multi-worker scaling, wake-storm behavior, connection-churn cost, large-body performance nor native bare-metal Linux rates.

## Syscall evidence

A [separate traced diagnostic](results/native-epoll-linux-20261027/syscalls) ran raw mode with one connection. Its throughput and traced timing are excluded from all performance results. The baseline collected 52,082 epoll waits, 104,182 `epoll_ctl` calls, 52,084 `timerfd_settime` calls and 52,090 reads; 52,081 reads returned an error, consistent with the unconditional nonblocking wake-descriptor read when no notification was present.

The candidate collected 139,674 `epoll_pwait2` calls, 16 `epoll_ctl` calls, one `timerfd_settime` call and ten reads. Those small residual setup/control counts also include the unchanged background I/O services. Normalized per wait, the baseline has roughly two control calls, one timer setup, one read and the wait itself. The candidate reduces the ordinary worker turn to roughly the wait itself and drains eventfd when notified. This confirms the removed work; it does not turn the traced run into throughput evidence.

## Validation and adoption

The Linux selector and Native test suites pass, including the existing cancellation, concurrent waiter, closure, stale-pointer retirement and backpressure tests. Four new tests cover notification retention/draining/reuse, cross-thread interruption, persistent edge delivery and synchronous deletion, reusable event storage, and finite/zero/infinite waits through both timeout interfaces. Targeted all-target/all-feature Clippy and formatting checks pass. Logs are retained with the evidence.

The prototype remains in an isolated worktree, and the original scheduler checkout's pending-file hashes are preserved. Production adoption still needs the project's complete source-region coverage gate, deterministic coverage of native OS-error paths, and kernel/seccomp compatibility review. Preserving sub-millisecond timeout behavior on kernels without `epoll_pwait2` needs a more complete fallback than this experiment's millisecond rounding. The compatibility fallback was functionally tested but is not the path measured on Linux 6.12.

The recommendation is to pursue the small native backend for predictable lower per-turn overhead, with the strongest measured case being sparse traffic. For improving the 64-connection HTTP benchmark specifically, these results favor profiling other costs next.

## Evidence and reproduction

- [Main raw samples](results/native-epoll-linux-20261027/main/samples.jsonl), [summary](results/native-epoll-linux-20261027/main/summary.json), and [environment](results/native-epoll-linux-20261027/main/environment.json).
- [Confirmation samples](results/native-epoll-linux-20261027/confirmation/samples.jsonl), [summary](results/native-epoll-linux-20261027/confirmation/summary.json), and [environment](results/native-epoll-linux-20261027/confirmation/environment.json).
- [Prototype patch](results/native-epoll-linux-20261027/candidate.patch), [source snapshots](results/native-epoll-linux-20261027/sources), [provenance](results/native-epoll-linux-20261027/provenance.json), and [artifact hashes](results/native-epoll-linux-20261027/hashes.json).
- [Linux runner](results/native-epoll-linux-20261027/linux_bench.py), original helper under `sources/base-http/benchmarks/hello/run.py`, and [syscall diagnostic](results/native-epoll-linux-20261027/trace_syscalls.py).
- [Functional test log](results/native-epoll-linux-20261027/native-tests.log) and [Clippy log](results/native-epoll-linux-20261027/native-clippy.log).

To reproduce, check out the scheduler baseline, place it beside the three saved HTTP source directories, and build the hello benchmark in release mode with its lockfile. Save that executable as `binaries/polling`. Apply `candidate.patch` at the scheduler root, rebuild with the same compiler/profile, and save the second executable as `binaries/native`. The runner expects this sibling directory layout and the baseline source hashes from `state.json`; the artifact includes that file. Run on a Linux host with CPUs 0–5 available, Python 3, wrk, taskset and Rust:

```sh
taskset -c 1 python3 linux_bench.py --output results/main --repetitions 5 --duration 8 --concurrency 1 16 64
taskset -c 1 python3 linux_bench.py --output results/confirmation --repetitions 5 --duration 12 --concurrency 64
```

Select fresh output directories: the runner refuses to overwrite an existing result directory. Run `trace_syscalls.py` separately after all timing completes. Binary SHA-256 values are `6dc812d8ebba7c54954c908b6948619bdf241f22f26c8bbda741332019ca8bd5` for baseline and `2ab7498e614bb1cd39a9ceb4cab50be794b940c2904d7b20f17727db4b8e75b1` for candidate. The original experiment and binaries are also retained at `/private/tmp/base-epoll-benchmark-3ey3dc46`.
