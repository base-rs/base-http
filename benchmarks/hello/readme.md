# Static HTTP/1 hello benchmark

A separate benchmark package comparing native Base HTTP with Hyper/Tokio. No Hyper or Tokio dependency is added to the HTTP libraries. The [server source](src/main.rs), [runner](run.py), lockfile and raw results make the comparison reproducible.

The [owned-receive experiment](owned-receive.md) changes only the raw server's receive API, retaining borrowed response writes. At 64 connections it improves paired throughput 8.62% over borrowed input and 5.42% over spawned Tokio, while increasing p99 latency. Single-connection CPU cost increases; production HTTP input remains unchanged.

The [equal offered-rate follow-up](fixed-rate.md) measures the same frozen controls at 200k, 220k and 240k requests/s. At 240k, borrowed and owned send-to-response p99 are approximately equal (339/344 µs). Scheduled-arrival p99 includes connection backlog and is 6.668/4.387 ms, with substantial variation between rounds. All 45 runs deliver within 0.03% of their targets and validate every response.

The [layered Linux investigation](layer-breakdown.md) separately compares task execution, driver polling, readiness, kernel event collection, and ready reads/writes. It identifies excessive empty polling for self-waking tasks and an extra io_uring receive per raw response; event collection and ready writes are close to Tokio. The [standalone controls](layers/readme.md) make those comparisons reproducible.

The [remaining Linux gap follow-up](raw-gap-followup.md) matches task context, counts socket/event/task operations and isolates HTTP processing in memory. Seven field-construction inline hints improve in-memory processing time by 10.7% and full HTTP user CPU per response by 3–9% across two cohorts. The longer paired throughput comparison still puts Base about 1.5% behind spawned Hyper, with mixed rounds. Scheduler experiments were reverted.

The [0.1.2 release comparison on Hana](scheduler-release-0.1.2.md) measures published scheduler 0.1.1 against 0.1.2 for Linux epoll and io_uring, with Hyper/Tokio as an interleaved control. The benchmark now resolves published 0.1.2 scheduler packages, so local scheduler edits do not affect it. The [previous release comparison](scheduler-release-0.1.1.md) covers 0.1.0 against 0.1.1 on macOS and a Linux VM.

The [Hana Linux investigation](hana-linux.md) measures epoll and io_uring on physical x86_64 hardware. It identifies redundant fallback accept/rearm calls, validates a default io_uring improvement, and evaluates opt-in TCP queue hints with their throughput and latency tradeoff. The experimental scheduler changes are preserved separately from this benchmark's published dependencies.

The [request target report](request-target.md) removes the codec's remaining request allocations and measures Linux instructions per request against Hyper/Tokio. The [raw read/write control](raw-io.md) bypasses all HTTP processing to compare Native and Tokio scheduler/I/O paths, including a spawned-supervisor control. It identifies readiness waiter bookkeeping and records a notifier change reducing raw user CPU 9.5% and complete HTTP user CPU 5.7% in the longer confirmation. The [HTTP ownership investigation](remaining-gap.md) isolates future ownership, corrects benchmark handler ownership, records rejected trailer layouts and measures the remaining Hyper lead. The [previous hot-path report](http-hot-path.md) covers writer registration, fused field facts, allocation-free empty exchanges and response planning against preserved binaries and interleaved Hyper controls. The [earlier cost-reduction report](hyper-target.md) covers borrowed wire planning, compact exchange state and inline fields.

The Hyper service follows the [official HTTP/1 hello-world guide](https://hyper.rs/guides/1/server/hello-world/), with connection supervision matched to Base. Load comes from the independent [wrk](https://github.com/wg/wrk) tool, not either HTTP client under test.

## Run

From this directory:

```sh
cargo build --release --locked
python3 run.py --wrk /absolute/path/to/wrk
```

On Linux, compare the released epoll and io_uring modes with Hyper:

```sh
python3 run.py --wrk /absolute/path/to/wrk \
  --backends base-native base-iouring hyper-tokio --output results/linux-recheck
```

The default run takes approximately four minutes after compilation. Options include `--backends`, `--concurrency`, `--repetitions`, `--duration`, `--warmup`, `--threads`, and `--output`. Files with matching names are overwritten; select a new output directory to retain earlier measurements. The runner starts isolated server processes and terminates only those processes after each sample. It records a JSONL journal, aggregate JSON, configuration and complete wrk output.

The initial macOS run uses wrk 4.2.0, revision `a211dd5a7050b1f9e8a9870b95513060e72ac4a0`. It was built in a temporary directory against the installed OpenSSL library:

```sh
git clone --depth 1 --branch 4.2.0 https://github.com/wg/wrk.git /tmp/base-http-wrk
make -C /tmp/base-http-wrk -j4 WITH_OPENSSL=/opt/homebrew/opt/openssl@3
```

Only plaintext HTTP is measured. OpenSSL is a build dependency of wrk, not part of the measured protocol.

## Matched workload

- Release builds with identical compiler/profile settings.
- HTTP/1.1 `GET /`, loopback TCP, persistent connections, one outstanding request per connection, no TLS or pipelining.
- Full HTTP modes serve static `200 OK`, `Content-Type: text/plain`, `Content-Length: 13`, body `Hello, world!`. Hyper's automatic Date field is disabled to match Base's response. Responses are constructed per request; neither server caches an encoded response.
- Both full HTTP services drop an unused request before returning a future, then construct a fresh response when that future is polled. Base uses a plain `Handler::call` returning an uncapturing async block, matching Hyper's closure.
- `TCP_NODELAY` is enabled on every accepted socket. Both sides have a 1,024 connection admission bound, above the measured loads.
- Each server has one application execution thread and one `FuturesUnordered` supervisor polling connection futures. No per-connection task is spawned. The accept loop preserves its outstanding accept when an existing connection finishes, as in `base_http_server::serve`. Base uses the actual `serve_connection` implementation and generic handler/body API.
- The native server supervisor executes on one native worker. The Base/Tokio and Hyper/Tokio supervisors execute on current-thread Tokio runtimes. The Direct variant has one foreground polling thread and a configured synchronous-work limit of one; its extra helper threads can consume CPU too. One application worker does not mean one total OS thread.
- Separate wrk processes use one load thread at one connection, four at 16 and 64 connections. Client and server run on the same machine.
- Three five-second measurements per backend/concurrency pair, each preceded by one second of warmup. A fresh server starts for every sample. Order is interleaved with fixed random seed 20261026. Reported aggregates are medians across repetitions; min/max throughput is retained.

Before every sample, an independent Python HTTP client checks body/status/fields, reuse for two requests on one socket and closure for a third. The runner rejects reported wrk socket/time-out and non-success status errors. Raw modes instead check three persistent replies and client-initiated closure, plus independent arbitrary-byte/reuse/EOF validation; they deliberately ignore request framing and `Connection: close`. Wrk does not validate response body bytes during the timed phase.

## Backend meanings

| Mode | HTTP implementation | I/O and execution |
| --- | --- | --- |
| `base-native` | Base HTTP/1 and application handler | Published `base-scheduler-native` 0.1.2, worker-owned readiness selector (direct kqueue on macOS and epoll on Linux), one native task worker. |
| `base-iouring` (Linux) | Same Base HTTP implementation | Published `base-scheduler-iouring` 0.1.2 with one Native task/ring worker and one readiness fallback worker. Default ring settings; borrowed payload buffers use nonblocking receive/send syscalls. |
| `base-iouring-raw` (Linux) | One read then one fixed response write | Same released io_uring configuration. |
| `base-iouring-owned-raw` (Linux) | Same raw loop | Uses `read_owned` with one reusable 16 KiB buffer; response writes remain borrowed. The borrowed Base raw controls use identical buffer storage. |
| `base-native-background` | Same Base HTTP implementation | Published Native executor with the standalone `async-io` readiness selector, preserving the previous reactor handoff for comparison. |
| `base-tokio` | Same Base HTTP implementation | Published `base-scheduler-tokio` 0.1.2 on a current-thread Tokio runtime. |
| `hyper-tokio` | Hyper HTTP/1 and `Full<Bytes>` body | Tokio TCP stream through `hyper-util::rt::TokioIo`, current-thread runtime. |
| `hyper-task-tokio` | Same Hyper HTTP implementation | Connection supervisor runs in one spawned task on the current-thread Tokio runtime, matching Base's spawned supervisor context. |
| `base-native-raw` | One read then one fixed response write | Native bound reader/writer, one native worker. |
| `base-tokio-raw` | Same raw loop | Base bound capabilities on current-thread Tokio. |
| `tokio-raw` | Same raw loop | Direct Tokio TCP, supervisor in the `block_on` root. |
| `tokio-task-raw` | Same raw loop | Direct Tokio TCP, supervisor spawned as one task. |
| `base-direct` | Same Base HTTP implementation | Published Direct backend, with its synchronous work/cleanup/retry services. |

The benchmark pins the interface and Direct, Native, Selector, Tokio and Linux io_uring packages to registry release 0.1.2; its lockfile fixes the transitive timer crate to the same release. Historical local-patch and 0.1.0 results retain their original provenance. Libraries in the parent directories retain their own development patches, which do not override this benchmark package's dependency resolution. The lockfile pins Hyper 1.12.0 and Tokio 1.54.1. Base/Tokio is a useful control, but a comparison against Hyper still includes differences in HTTP handling, I/O bindings, allocation and write batching. It does not isolate the semantic message model's cost.

## Interpretation

The [results](results.md) describe one host and workload. Latency is wrk's closed-loop distribution, not a fixed-arrival-rate overload test. Aggregate p99 is the median of per-run p99 values, not a pooled percentile. CPU readings are process CPU-time deltas from `ps` over the measurement interval, with 100% meaning one core; RSS is observed after the run, not peak memory. CPU affinity is not controlled, and normal host activity can introduce variation.

This benchmark checks small persistent static responses. It does not establish multi-worker scaling, connection-churn cost, large/streaming body performance, proxy behavior, TLS, HTTP/2/3 or production tail latency. On macOS, `base-native` uses readiness I/O, not io_uring. The Hana release comparison measures Linux epoll and io_uring separately with controlled affinity and whole-process CPU accounting.

## Profiling

The [profiling findings](profiling.md) cover the initial macOS stack samples, codec allocation counts, and isolated write-coalescing/buffer-reuse experiments. General output batching and allocation reductions are now integrated; see the [production HTTP comparison](http-optimization.md).

```sh
cargo build --release --locked --features profile-tools --bins
python3 profile.py --wrk /absolute/path/to/wrk
python3 summarize_profile.py
target/release/allocation-probe
python3 experiment.py --wrk /absolute/path/to/wrk
```

`profile.py` uses macOS `/usr/bin/sample`; its samples include blocked time and are not on-CPU percentages. `summarize_profile.py` selects the application thread and computes disjoint exclusive categories. The allocation probe uses in-memory input/output, after warming connection-local storage, and excludes networking and runtime allocation.

The historical `experiment.py` builds four disposable codec copies under `target/profile-experiments` before timing anything. It saves the patches, allocation counts, source/binary hashes, preflight observations and raw wrk measurements under `results/experiments`. The coalescing treatment is deliberately restricted to this static workload; it does not implement generic streaming, interim or upgrade policy. `--output` selects a fresh results directory and `--skip-build` reuses existing diagnostic executables. Avoid running compilation or a profiler during these timing experiments.

The [follow-up findings](profiling-followup.md) cover worker-owned readiness and the remaining HTTP ownership costs. `experiment_next.py` builds further disposable HTTP/scheduler variants, `validate_http.py` checks general HTTP changes independently of the static benchmark patches, and `profile.py --no-sample --seconds 0` records five seconds of process user/kernel CPU accounting without a stack sampler. `summarize_profile.py --directory ... --backends ...` handles the additional profiles. Full reproduction commands and limitations are in the follow-up document.


## Local scheduler implementation

The [worker-owned kqueue implementation and results](scheduler.md) describe the local scheduler fix, lifecycle validation, and unchanged-HTTP comparisons. `compare_local_scheduler.py` includes the previous background-reactor arrangement as a matched control. Historical result directories retain their original registry/experimental provenance.

The subsequent [readiness policy and results](readiness.md) remove the ordinary redundant receive from the Native scalar stream path. The report includes urgent-data/EOF correctness checks, socket-call counts and an uninstrumented before/after/Hyper confirmation.

## Integrated HTTP optimizations

[Production integration and results](http-optimization.md) documents the bounded batching and allocation changes, their semantic regressions, and a comparison against saved production binaries using the same local scheduler. From this directory, after preserving a pre-change binary and building the revised code:

```sh
python3 compare_http.py --wrk /absolute/path/to/wrk \
  --before-binary /absolute/path/to/before/base-http-hello-benchmark \
  --binary target/release/base-http-hello-benchmark \
  --output results/http-recheck --repetitions 5 --duration 8
```

The comparison interleaves the before/after Native and Base/Tokio modes plus Hyper/Tokio, using the same handler and independent preflight checks. The original experimental patch scripts expect the earlier source layout; use their saved staged trees or the preserved pre-integration sources when reproducing historical experiments. They are not patch applicators for the revised production code.
