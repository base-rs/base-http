# HTTP performance

Updated 2026-10-30. This document records the performance model, measured results and open questions for the Base HTTP stack. Dated reports and raw artifacts remain the evidence for individual experiments.

The current HTTP/1 client has a measurable orchestration cost above its connection-only codec/transport path. Isolated pool admission accounts for a small part of that cost. Recent changes remove unnecessary maintenance wakes, cancellation subscriptions and copies of inactive future storage without changing public HTTP interfaces or duplex ownership.

The latest full comparison on macOS puts pooled Base **6.4% behind Hyper for HTTP and 11.0% for HTTPS at 32 persistent connections**. An earlier run of the same executables showed 2.1%/2.3% gaps. The approximately 2% result did not reproduce consistently; neither cohort establishes a stable performance gap or a general backend ordering. On Linux with the [released dependencies](#uncontended-admission-cancellation-registration-and-scheme-defaults-2026-10-30), pooled Base completes about 10.6% fewer HTTP responses per second than Hyper at 32 concurrent requests, while executing 30% fewer user instructions per response. The Hyper rounds ran after the Base rounds rather than interleaved with them.

## Scope and architecture

The goal is native HTTP over Base scheduler capabilities, including Linux io_uring, while preserving streaming, full duplex, cancellation and explicit resource retirement. Hyper/Tokio are benchmark controls rather than production dependencies of the HTTP libraries. HTTP/1 supports plain TCP and rustls TLS; HTTP/2/3 are future work.

The established client path has these layers:

| Layer | Responsibility | Performance boundary |
|---|---|---|
| `base-http` | Message metadata, ordered fields and body frames | Semantic construction and mapping perform no I/O. Header interpretation is explicit application work. |
| `base-http-client` | Remote endpoint, admission, exchange ownership and supervision | Direct HTTP/1 operations run in the application future. |
| `base-pool` | Connection construction, reservations, active uses and retirement | HTTP/1 uses multiplex admission with capacity one; admission is separate from protocol progress. |
| `base-http1` | Retained request/response codecs, framing and validation | Typed transport operations; codec buffers survive connection reuse. |
| Endpoint/TLS/socket | Establishment, ALPN, optional encryption and I/O | TLS sits below the HTTP/1 codec. Scheduler-bound readers/writers retain transport state. |
| Scheduler | Task execution, readiness or completion delivery, cancellation and cleanup | Backend policy must preserve progress and buffer ownership. |

For imperative `Client::open_request`, acquisition is followed by direct connection use. The exchange independently owns request output and response input. Writes drive output; response body reads drive input. Both codec directions return before the active pool use completes. Receiving response metadata, or reaching response EOF, does not finish an active upload.

The explicit client driver handles establishment, queued calls, deferred closure and retirement, plus supervised connection/TLS work. Ordinary established direct requests require no per-request supervisor acknowledgment. Direct TLS reads/writes also make progress in application futures. Connection supervision is currently scoped under one client driver; this prototype does not establish independently scheduled multiworker connection drivers.

The latest pooled benchmark measures the imperative **`base-open` path**. The queued pull-body **`base-call` path** is a separate implementation and is not included in the latest comparison. Erasure occurs at the client adapter boundary; transport operations remain statically dispatched. There is no body-data queue or new boxed future per frame on the direct path.

See [client/connection taxonomy](design/client-connection.md), [transport layering](design/transport.md), [duplex ownership](design/duplex.md) and the [client implementation notes](crates/client/readme.md).

## Latest full client comparison

The [latest rerun](benchmarks/client/orchestration-full-rerun-20261029.md) uses three repetitions, two-second timed phases and 0.5 seconds of warmup. C1/C32 means one or 32 persistent connections. Each lane has one outstanding request, with no pipelining. The workload is an empty GET and a validated 11-byte response over loopback.

All clients use the same independent two-worker Hyper server. Each client has one worker, retaining its implementation's actual orchestration. TLS identity, ALPN, version and resumption settings match. Measurements are from a shared Apple M4 Pro Mac, without CPU affinity. Builds, tests and profiling are kept outside timing.

Median validated responses per second:

| Client | HTTP C1 | HTTP C32 | HTTPS C1 | HTTPS C32 |
|---|---:|---:|---:|---:|
| Previous Base pooled | 34,797 | 96,578 | 30,716 | 91,989 |
| Updated Base pooled | 35,191 | 106,733 | 31,142 | 93,690 |
| Hyper pooled | 38,551 | 114,018 | 31,100 | 105,254 |
| Base connection-only | 35,768 | 130,419 | 32,711 | 107,561 |
| Hyper connection-only | 38,866 | 117,567 | 31,403 | 114,203 |

Median client CPU microseconds per response at C32:

| Client | HTTP | HTTPS |
|---|---:|---:|
| Previous Base pooled | 10.325 | 10.837 |
| Updated Base pooled | 9.352 | 10.642 |
| Hyper pooled | 8.751 | 9.478 |
| Base connection-only | 7.626 | 9.257 |
| Hyper connection-only | 8.485 | 8.737 |

All **60 samples**, **8,505,240 measured responses** and **2,123,146 warmup responses** passed body validation and exact request accounting. Every sample accepted the specified number of connections, with zero server failures. Normal timing builds contain no allocation counters.

Raw [summary and ranges](benchmarks/client/results/orchestration-full-rerun-20261029/summary.json), [machine/compiler metadata](benchmarks/client/results/orchestration-full-rerun-20261029/metadata.json), [audit](benchmarks/client/results/orchestration-full-rerun-20261029/audit.json), [measured sources](benchmarks/client/results/orchestration-full-rerun-20261029/sources.tar.gz) and [provenance](benchmarks/client/results/orchestration-full-rerun-20261029/provenance.json) are retained.

### Variation between identical builds

Both full cohorts use the same frozen client executables and original measured source archive:

| Cohort, C32 | Base HTTP | Hyper HTTP | Base gap | Base HTTPS | Hyper HTTPS | Base gap |
|---|---:|---:|---:|---:|---:|---:|
| First full comparison | 116,873 | 119,370 | 2.1% | 107,420 | 109,901 | 2.3% |
| Latest rerun | 106,733 | 114,018 | 6.4% | 93,690 | 105,254 | 11.0% |

Within the latest rerun, pooled Base ranges from 90,759–107,316 HTTP responses/s and 88,539–101,408 HTTPS responses/s. Hyper ranges from 110,599–118,072 and 101,159–108,782 respectively. The cause of the unequal run variation has not been isolated. A change in the apparent gap between these cohorts is not another source change or a demonstrated regression.

A separate, tighter [five-pair before/after repeat](benchmarks/client/results/orchestration-final-repeat-20261029/paired-analysis.json) shows median paired throughput gains of **2.15% HTTP and 1.54% HTTPS**, with approximately matching CPU reductions. Four of five pairs improve for each transport. HTTP pair changes range from −5.2% to +8.0%; HTTPS from −1.6% to +5.7%. Aggregate HTTPS medians move slightly backward despite four improving local pairs. Preserve both aggregates and pairs; the evidence supports a modest improvement with material variation.

The [original orchestration report](benchmarks/client/orchestration-20261029.md) retains that experiment separately. Its 80 final timing samples validate 17,434,573 measured responses. Do not pool those samples with the latest rerun to manufacture a more precise result.

## What has been isolated and changed

### Pool admission and maintenance

The isolated pool workload completes an active use, reacquires its capacity and activates the reservation. It contains no HTTP, sockets, exchange construction or erased body polling. The optional retirement observer is explicitly repolled after every cycle.

Median CPU microseconds per cycle across three repetitions:

| Resources | Retirement observer | Before | After |
|---|---|---:|---:|
| 1 | No | 0.1074 | 0.0842 |
| 1 | Yes | 0.1578 | 0.0953 |
| 32 | No | 0.1557 | 0.1267 |
| 32 | Yes | 0.2200 | 0.1388 |

The observed cycle generates one retirement wake before and zero afterward. Timer and loop overhead are included; this is an isolation control, not a cost to subtract directly from HTTP timings. The warm capacity-one cycle allocates nothing. The initial hypothesis that removing/reinserting the sole B-tree entry allocated another leaf was disproved: Rust retains the empty root leaf.

The retained changes are:

- Update a reservation from Reserved to Active in place. Rejected activation still removes the reservation and returns admission.
- Avoid notifications on immediate acquisition with no queued waiters and on successful activation. Queued acquisition still wakes successors.
- Give retirement its own event. Cleanup wakes when work is queued/requeued, shutdown starts, construction aborts or retirement is acknowledged. Normal reuse does not wake cleanup.
- Wake the HTTP supervisor on accepted-request completion only when a closed client reaches zero pending exchanges. Submission and deferred cleanup notify independently.

FIFO admission, consumable opening credit, abandoned-use accounting and explicit cleanup acknowledgments remain intact. See [pool samples](benchmarks/client/results/orchestration-ablation-20261029/pool-samples.json), [pool summary](benchmarks/client/results/orchestration-ablation-20261029/pool-summary.json) and the [pool lifecycle notes](../base-pool/design/port.md).

### Cancellation and future-state copies

The cancellation wrapper now stores its nested future directly in concrete pinned state. It checks cancellation before polling work, then subscribes to cancellation only if work returns Pending. Polling cancellation after Pending also observes cancellation racing the initial check. Already-cancelled operations do not start.

Cache initialization and the private sequential request sink use generated pinned `project_replace` transitions. On the measured Rust 1.87/arm64 build, `Pin::set` had emitted copies of inactive future payloads. The inspected changes remove approximately 7 KiB/6 KiB cache-initialization copies and further approximately 3.5 KiB/2.9 KiB first-poll copies from sink routines. The final inspected HTTP sink completion routine retains only 288-byte codec-owner memcpy calls; its TLS variant has none. This describes those routines, not the entire request path or a sum of bytes copied per request.

The [assembly extracts](benchmarks/client/results/orchestration-assembly-20261029/) and [review patch](benchmarks/client/results/orchestration-final-full-20261029/production-review.patch) retain the evidence. These changes preserve pinned destruction, independent upload/download and distinct completion/cancellation identities. An old completed handle must never be able to cancel a later exchange.

### Allocation and ownership work

Earlier client work retained connection codecs and their buffers, released admission synchronously once both directions returned, reduced exchange wrapper allocations, and cached empty pinned operation storage. Each connection retains at most one empty allocation for its opening future, request sink and response stream. Cached storage contains no live exchange captures and uses weak return links to avoid ownership cycles.

Rounded whole-client allocation calls per steady-state response in the documented diagnostic stages:

| Stage | HTTP | HTTPS |
|---|---:|---:|
| Before exchange/layout reductions | 19 | 21 |
| Compact exchange adapters | 15 | 17 |
| Reused operation storage | 12 | 14 |
| Current orchestration changes | 12 | 14 |
| Current Base connection-only control | 6 | 8 |

The current orchestration improvement does not reduce allocation count or requested-byte traffic. It removes work inside retained storage and notification machinery. Counts alone do not capture copies, locking, wakeups or task progress. Counter-enabled executables are diagnostics, not throughput evidence.

History and measured limitations are preserved in the [connection/TLS work](benchmarks/client/performance-20261028.md), [exchange allocation work](benchmarks/client/exchange-overhead-20261029.md), [storage reuse investigation](benchmarks/client/exchange-reuse-20261029.md) and [current allocation diagnostic](benchmarks/client/results/orchestration-final-allocations-20261029/).

### Uncontended admission, cancellation registration and scheme defaults (2026-10-30)

This was measured before the pool and scheduler changes were released, on the Linux host described below, with the pooled HTTP/1 client over TCP at 32 concurrent requests. The server ran on CPUs 2, 3, 6 and 7 and the client on CPU 1. All sources were working trees copied without git metadata, so none of the base-http, base-endpoint, base-pool or base-scheduler revisions were recorded. The pool and scheduler trees were applied as Cargo path patches; the [environment notes](benchmarks/client/results/admission-scheme-20261030/environment.txt) list the patches, binary hashes and commands. The first three builds were interleaved over 10 rounds of four seconds. The scheme change was measured in an earlier six-round interleaving of the same binaries plus the scheme build. Each figure is the median per response, with the range in brackets:

| Build | User instructions | Cycles | L1 data misses |
|---|---:|---:|---:|
| Baseline | 19,479 [19,476–19,481] | 17,835 [17,541–18,377] | 586 [578–593] |
| Pool takes available capacity before listening and skips busy slots | 18,907 [18,903–18,911] | 17,713 [17,376–18,866] | 562 [551–567] |
| Cancellation children in reusable slots | 18,663 [18,660–18,666] | 17,482 [17,244–18,369] | 554 [548–558] |
| Requests take scheme constants (six rounds) | 18,576 [18,574–18,577] | 17,739 [17,327–18,456] | 550 [545–554] |

In the six-round run the cancellation build measured 18,663 instructions and 17,280 cycles, so the scheme change saves about 87 instructions while its cycle difference is within noise. The [10-round rows](benchmarks/client/results/admission-scheme-20261030/pool-cancellation.txt) and [six-round rows](benchmarks/client/results/admission-scheme-20261030/scheme.txt) are kept. Instruction counts vary by less than 10 between runs. Responses per second varied by about ±3% between runs of the same build, which is larger than the cycle differences, so throughput is not claimed. The pool and cancellation changes are in base-pool and base-scheduler. The client now fills a request's default scheme from the `Scheme::HTTP` and `Scheme::HTTPS` constants, instead of converting the endpoint's text with a case-insensitive comparison.

These attempts were rejected:

- **A synchronous `try_acquire` for established connections.** It saved about 245 more instructions per response, but cycles did not fall, and it added pool API.
- **One exchange cancellation per connection.** This is unsafe: `RequestBodyWriter::abort()` and other late handles can cancel after completion, and would cancel the next exchange on that connection.
- **Yielding before each direct request write, to batch sends.** It reduced server sleep and wake cycles from about 0.43 to 0.33–0.37 per response. However, client user time rose from about 4.1 to 4.7 µs per response and throughput fell slightly.

The following observations come from exploratory `perf stat`, `strace -c` and `nstat` samples of the cancellation build and of Hyper. Their rows were not archived, so they have no ranges and guide the next investigation rather than establishing a result. After these changes, pooled Base used less user CPU per response than Hyper: about 4.0 µs against 4.3 µs. Hyper still completed more responses, about 117,000 per second against about 103,000. The remaining difference was system time, about 3.56 µs against 3.07 µs per response. Both clients made one send and one receive per response, with matching TCP segment counts, so the syscall count did not explain it. However, the server slept and woke about 0.47 times per response under Base and 0.16 under Hyper, which suggests the cost is in wakeups.

The [released-dependency measurement](benchmarks/client/results/released-dependencies-20261030/environment.txt) repeats the comparison with frozen builds. The baseline is base-http 519d476 with the published base-scheduler 0.2.0 and base-pool 0.2.0, and with base-endpoint d7625aa from a path checkout, because base-endpoint was not yet published. The candidate is this revision with base-scheduler 0.3.0, base-pool 0.3.0 and base-endpoint 0.1.0, and includes the scheme change. The host is an otherwise idle Intel Xeon E3-1240 v6 on Linux 7.2.11 with rustc 1.99.1, using release builds and the CPU placement above. The two builds were interleaved over 10 rounds of four seconds after one second of warmup, followed by 10 rounds of Hyper from the candidate binary. Each figure is the median per response, with the range in brackets:

| Build | User instructions | Cycles | L1 data misses | Responses per second |
|---|---:|---:|---:|---:|
| Baseline | 19,932 [19,931–19,935] | 18,432 [18,113–19,132] | 589 [586–594] | 100,052 [96,483–101,533] |
| Released dependencies | 18,552 [18,549–18,556] | 16,787 [15,941–17,403] | 546 [543–550] | 105,126 [102,517–108,241] |
| Hyper | 26,457 [26,405–26,512] | 16,570 [16,311–16,860] | 350 [345–357] | 117,570 [115,965–119,342] |

User instructions fall by 6.9% and cycles by 8.9%, and neither range overlaps. Throughput rises by about 5%, and its ranges do not overlap either. The paired gain in each round ranges from 1.8% to 9.4%, and two of the 10 pairs are within the ±3% variation. This run did not repeat the baseline against itself, so the throughput gain is not claimed. The baseline counts more instructions than the earlier baseline above because it is a different build: base-http 519d476 with the released 0.2 pool and scheduler, rather than the unreleased working trees that the earlier experiment patched in; compare figures only within one table. This run recorded counters and throughput only, not system time or context switches. The [raw rows](benchmarks/client/results/released-dependencies-20261030/base-open.txt), [Hyper rows](benchmarks/client/results/released-dependencies-20261030/hyper.txt), binary hashes, the baseline lockfile and the measurement scripts are kept beside it. The Hyper rounds ran after the Base rounds rather than interleaved with them. The baseline lockfile adds the `smallvec` entry for `base-http1` that its revision omitted, without changing any version.

## Scheduler, transport and HTTP controls

Different workloads answer different questions. Their backend versions, task layouts, hosts, buffer ownership and CPU accounting can differ; use each report's metadata and matched controls.

| Benchmark | Question | Evidence |
|---|---|---|
| Pooled client versus connection-only | What does admission, exchange ownership and client orchestration add? | [Client harness](benchmarks/client/readme.md) and the full comparisons above. |
| Static HTTP/1 server | What does the complete server cost against Hyper with an independent load generator? | [Hello harness](benchmarks/hello/readme.md). |
| Raw read/fixed-response server | Does a gap remain when parsing and application semantics are removed? | [Raw I/O control](benchmarks/hello/raw-io.md). |
| Echo | How do scheduler and socket paths behave with useful per-connection work? | [Echo results](benchmarks/echo/results.md) and [method](benchmarks/echo/readme.md). |
| Task/readiness/event/ready-I/O controls | Which mechanisms perform additional operations? | [Layer breakdown](benchmarks/hello/layer-breakdown.md) and [standalone controls](benchmarks/hello/layers/readme.md). |
| Fixed offered-rate traffic | How do latency and backlog compare at the same request rate? | [Fixed-rate study](benchmarks/hello/fixed-rate.md). |
| Worker/reactor progress studies | Where does elapsed time accumulate between scheduling, issuance and consumption? | [Execution model](benchmarks/hello/execution-model.md), [task topology](benchmarks/hello/task-topology.md) and [receive lifecycle](benchmarks/hello/receive-lifecycle.md). |

The [scheduler overhead notes](overhead.md) retain the Linux epoll/io_uring research. It identified an extra borrowed receive attempt and completion-batching policy as real workload costs. Synthetic self-waking controls exposed excessive empty driver turns, but echo did not reproduce the same large scheduling gap. These are separate findings. macOS client results here do not measure Linux io_uring or prove that scheduler overhead is eliminated.

Retain failed experiments as well as improvements. Examples include mutex-backed operation caches that reduced allocations but regressed throughput, unconditional poll-first owned receives that worsened CPU/latency, and submission-deferral changes that did not justify becoming the default. Their dated reports explain why fewer allocations or fewer syscalls were insufficient evidence.

## Correctness and evidence requirements

Performance changes must preserve full duplex and proxy backpressure, cancellation isolation, no replay of partially written output, stable pinned state, and pool charges through actual protocol/transport cleanup. Returning a response must not release an unfinished upload. Dropping a handle is not proof of asynchronous retirement.

The final client passes 44 integration tests with rustls and eight unit tests; its default configuration passes 37 integration tests and eight unit tests. The pool passes 27 behavior tests, one TCP integration test and two doctests. New regressions check actual wake delivery and silence during normal reuse, cancellation races, pinned addresses and exactly-once destruction. Client/pool/benchmark Clippy and formatting pass. The client also passes against the published pool without a Cargo patch. [Validation logs](benchmarks/client/results/orchestration-final-validation-20261029/) and the [validation supplement](benchmarks/client/results/orchestration-final-full-20261029/validation-supplement.tar.gz) are retained.

For every new timing claim:

1. Freeze executables and record their hashes, source archives, lockfiles, compiler, features and backend configuration. Check that archives belong to the measured binaries.
2. Keep instrumented allocation/syscall/profile runs separate from ordinary timing. Do not compile or test alongside timing. Stack samples containing blocked time are not on-CPU percentages.
3. Interleave matched controls, keep every sample and report medians, ranges and paired results where applicable. Mark contaminated exploratory samples explicitly; do not count them as clean confirmation.
4. Validate payloads, request totals, connection counts and failures. Keep setup/warmup/cleanup inclusion explicit for CPU accounting.
5. Compare latency at the same offered rate. Saturation P99 at different achieved rates is not an equal-load comparison. Distinguish send-to-response RTT from scheduled-arrival latency, which includes backlog.

## Reproduction and dependency status

The workspace and benchmarks depend on the published base-scheduler 0.3.0, base-pool 0.3.0 and base-endpoint 0.1.0, so ordinary locked builds reproduce the released-dependency candidate without Cargo patches or sibling checkouts. Rebuilding its baseline needs a base-endpoint checkout at d7625aa, as its environment notes record. The full client comparison above was measured earlier with a local base-pool patch; its frozen executables are listed below.

Frozen normal executables:

| Implementation | Local artifact | SHA-256 |
|---|---|---|
| Updated | `/tmp/base-http-orchestration-final-20261029` | `66fe13a6e9305f41fac8bf21cf20653c461ffed8b4b366d24ce8e8c378ba2dd2` |
| Previous | `/tmp/base-http-exchange-reuse-final-20261029` | `d6e9658443587be61afa277065f39c962c23e28d5196581eb3d07fd091c42329` |

While these local artifacts remain available, repeat the full comparison with a new output directory:

```sh
python3 benchmarks/client/results/orchestration-full-rerun-20261029/rerun.py \
  --binary /tmp/base-http-orchestration-final-20261029 \
  --output benchmarks/client/results/performance-full-new \
  --baseline-binary /tmp/base-http-exchange-reuse-final-20261029 \
  --baseline-sources benchmarks/client/results/exchange-reuse-final-20261029/sources.tar.gz \
  --repeats 3 --seconds 2 --warmup 0.5 --concurrency 1,32 \
  --implementations before-open,base-open,base-connection,hyper,hyper-connection
```

The [wrapper](benchmarks/client/results/orchestration-full-rerun-20261029/rerun.py) verifies binary hashes and retains the original measured sources. It does not rebuild or snapshot a different working tree as though it produced those binaries.

To measure a newly built candidate from the current working tree:

```sh
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/base-http-validation-target \
  rustup run 1.87.0 cargo build --locked --manifest-path benchmarks/client/Cargo.toml --release
cp /tmp/base-http-validation-target/release/base-http-client-benchmark /tmp/base-http-client-candidate

python3 benchmarks/client/run.py \
  --binary /tmp/base-http-client-candidate \
  --output benchmarks/client/results/performance-candidate-new \
  --baseline-binary /tmp/base-http-exchange-reuse-final-20261029 \
  --baseline-sources benchmarks/client/results/exchange-reuse-final-20261029/sources.tar.gz \
  --repeats 5 --seconds 4 --warmup 0.5 --concurrency 32 \
  --implementations before-open,base-open
```

`run.py` records metadata with the Rust 1.87.0 toolchain and macOS `sysctl`, so build with the same toolchain on macOS. Substitute a frozen baseline binary and its source archive; the paths shown are those of the earlier experiment and are not kept in the repository. Preserve the measured lockfile in the result archive. A new working-tree build is a new experiment, even when it uses the same command. On a Linux host, `cmp.sh` in the [released-dependency results](benchmarks/client/results/released-dependencies-20261030/) interleaves frozen binaries under `perf stat` with the CPU placement above.

For isolated pool cycles, use `binary pool --implementation pool-observed --concurrency 32 --seconds 2 --warmup 0.3`, or `--implementation pool` without the observer. For allocations, build separately with `--features count-allocations`, preserve that executable under a different name and do not use its throughput for performance claims.

## Next investigations

1. **Stabilize comparison conditions.** Use longer balanced interleaving, control task/client placement and investigate the unequal variation between identical builds. Repeat on idle physical Linux hardware with explicit CPU placement before claiming epoll/io_uring performance from this client work.
2. **Reduce server wakeups from pooled requests.** Compare when Base and Hyper issue requests across connections within one scheduler turn. Base polls every lane in one task; Hyper spawns one task per lane. Measure the change with system time and server context switches, not only with user instructions.
3. **Separate the remaining client costs.** Isolate metadata defaults, cancellation-tree registration, completion state, codec ownership transfer and adapter construction. Connection-only bypasses all of these as well as pooling, so subtracting its CPU from pooled CPU does not identify pool overhead.
4. **Reduce construction without weakening ownership.** Inspect remaining metadata cloning, allocation traffic and generated copies. Preserve distinct exchange identities while old handles remain alive. Test candidate changes independently against the same frozen baseline.
5. **Broaden the workload after a confirmed gain.** Measure known/unknown-length POST bodies, larger responses, concurrent upload/download, proxy backpressure, fresh connections and TLS handshakes. Test independently scheduled multiworker connections before drawing scaling conclusions.

HTTP/2 multiplexing, QUIC and HTTP/3 will introduce different progress and flow-control costs. The current small-response HTTP/1 results do not establish their performance or justify changing their interfaces.
