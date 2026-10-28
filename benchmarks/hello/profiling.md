# Static hello: profiling findings

Measured 2026-10-26 on the same Apple M4 Pro/macOS host as the [baseline](results.md). **Socket output is the largest demonstrated opportunity.** Base sends the response head and its 13-byte body separately. Combining those writes substantially improves throughput. Receive-buffer reuse removes most allocated bytes but has a much smaller throughput effect in this workload.

Production HTTP and scheduler sources were unchanged during these initial experiments. General HTTP batching and allocation reductions are now integrated; see the [current production comparison](http-optimization.md). Experimental codec changes live in disposable copies under `target/profile-experiments`; their patches and measurements are retained separately.

The [follow-up profile](profiling-followup.md) investigates the remaining gap with worker-owned readiness, reduced HTTP copying and process user/kernel CPU accounting.

## Where the application thread is sampled

Eight-second macOS `sample` captures, requested at 1 ms intervals, during independent wrk load at 64 persistent connections:

| Exclusive sample category | Base/native | Base/Tokio | Hyper/Tokio |
| --- | ---: | ---: | ---: |
| Send syscalls | 66.3% | 71.0% | 71.9% |
| Receive syscalls | 17.1% | 11.6% | 18.6% |
| Allocation/deallocation | 3.6% | 4.5% | 0.5% |
| Wait/readiness syscalls | 3.4% | 0.8% | 1.7% |
| Explicit cancellation/event/mutex work | 0.6% | 0.9% | 0.4% |
| Remaining user code | 9.0% | 11.2% | 6.9% |

The selected threads contain 5,343, 5,899 and 6,007 samples respectively. Categories are disjoint exclusive counts, calculated by subtracting each node's immediate children. The native application runs on `base-native-task`; Tokio variants run on the main thread. The native root and blocking-pool threads are parked and are excluded. Native's additional async-io reactor work is outside this application-thread table but is included in process CPU measurements. One Hyper sample could not be unwound and is unattributed (<0.1%).

**These are wall-clock stack observations, including blocked/off-CPU intervals, not on-CPU CPU percentages.** The profiler perturbs throughput; profiled throughput is not used for the performance comparison below. Symbol inlining/unwinding and broad categories limit precision. In particular, the cancellation category does not capture all executor/I/O overhead.

The leading exclusive symbols are `__sendto` for Base (3,541 native samples; 4,186 Tokio samples) and `writev` for Hyper (4,320 samples), followed by `__recvfrom`. Both spend a large fraction of samples in socket output, but this says nothing about equal cost per request. Base makes two writer calls for this response; Hyper's captured output path uses vectored output. The isolated experiment below tests the effect of combining Base's writes.

Raw [native](results/profiles/base-native.sample.txt), [Base/Tokio](results/profiles/base-tokio.sample.txt) and [Hyper](results/profiles/hyper-tokio.sample.txt) captures, demangled counterparts, [category/symbol summary](results/profiles/summary.json), and [baseline binary provenance](results/profiles/provenance.json) are retained. The profiled executable's hash matches the original benchmark executable.

## Isolated throughput experiments

Four copies of the actual HTTP/1 codec were built with the same release configuration and lockfile:

- **Baseline:** unchanged codec.
- **Receive reuse:** allocate the zeroed 16 KiB read scratch buffer once per connection and reuse it. Retain the existing copy into the parser buffer.
- **Coalesced:** reuse a connection-local output buffer, append the response head and body, and flush them together. This diagnostic explicitly assumes the benchmark's immediately available, fixed 13-byte body. It retains per-request parsing, response construction, framing checks and serialization.
- **Combined:** both changes.

All compilation finished before timing. Each Base variant ran on both native and Tokio, with an unchanged Hyper control: 27 interleaved samples, three five-second measurements per combination after one-second warmup, 64 connections and four wrk threads. A fresh process was used per sample; order was shuffled within each repetition with seed 20261027. See [environment](results/experiments/environment.json), [source/binary hashes](results/experiments/sources.json), [sample journal](results/experiments/samples.jsonl), and [aggregate results](results/experiments/summary.json).

| Variant | Base/native req/s, median (min–max) | Base/Tokio req/s, median (min–max) |
| --- | ---: | ---: |
| Baseline | 61,090 (57,874–61,880) | 68,442 (68,408–69,304) |
| Receive reuse | 60,581 (60,413–61,706) | 71,909 (66,702–72,344) |
| Coalesced | 83,461 (83,157–84,985) | 104,101 (102,476–107,545) |
| Combined | 86,700 (85,016–87,146) | 111,404 (104,794–111,664) |

Hyper/Tokio's unchanged control reached 126,974 req/s (123,990–130,326). Coalescing alone improved Base/native by **36.6%** and Base/Tokio by **52.1%** over contemporaneous baselines. Combining both changes improved them by **41.9%** and **62.8%**. Base/Tokio with both changes reached 87.7% of the Hyper control's throughput, compared with 53.9% at baseline. These are ratios of medians, not an additive decomposition of the remaining gap.

Receive reuse alone changed median throughput by −0.8% on native and +5.1% on Tokio, with overlapping baseline/treatment ranges. It does not show a consistent improvement across backends in these short runs. The robust, larger improvement comes from coalescing. Native's process CPU rose from 118.9% at baseline to 126.1% with coalescing, while Tokio remained near one core; batching also reduced CPU per request. The native helper/reactor costs are still relevant to its gap with Tokio.

Median per-run p99 latency fell from 1.360 to 0.920 ms on native and from 1.240 to 0.797 ms on Base/Tokio with coalescing; combined results were 0.910 and 0.777 ms. Hyper's p99 was 0.693 ms. These remain closed-loop wrk measurements, not production tail-latency claims.

Each sample passed independent status/body/fields, connection reuse and explicit-close checks. Across 11,803,563 timed requests, wrk reported zero socket/timeout or non-success status errors. Wrk does not check body bytes during timing. Base stderr contains peer-reset errors consistent with load-generator teardown; raw stderr and warmup/measured output are retained in each variant's results directory. Normal host activity and lack of CPU affinity limit precision; small differences should not be treated as proven improvements.

## Allocations and copies

An in-memory probe executes 10,000 exchanges through the actual `RequestReader` and `ResponseWriter`, including per-request static response construction, after warming connection-local capacities. Input supplies one realistic GET per read; a nonallocating output sink counts writer calls. It excludes socket, runtime and supervisor allocation. These are allocated bytes, not retained or peak memory.

| Codec variant | Allocations/exchange | Allocated bytes/exchange | Writer calls/response |
| --- | ---: | ---: | ---: |
| Baseline | 46 | 20,908 | 2 |
| Receive reuse | 45 | 4,524 | 2 |
| Coalesced | 46 | 20,908 | 1 |
| Combined | 45 | 4,524 | 1 |

Receive reuse removes exactly 16,384 allocated bytes per exchange, a 78.4% reduction, while removing only one allocation. Coalescing uses retained connection-local capacity after warmup, so its steady-state allocation counts remain unchanged. The sink counts API calls; short socket writes could require additional syscalls on real transports.

The remaining allocation/copy sites include a fresh 100-entry `httparse::Header` vector per message (about 3.2 KiB on this host), copying the raw head, creating owned field names/values, cloning the request head for `Exchange`, constructing the buffered body, cloning/rebuilding the response fields during framing, and creating/growing the serialized response-head vector. These are visible in [input](../../crates/http1/src/input.rs), [output](../../crates/http1/src/output.rs), [fields](../../src/fields.rs) and [buffered body](../../src/body/buffered.rs). The probe measures their total, not their individual time contribution. This workload largely uses raw fields; the captures do not establish lazy semantic indexing as a bottleneck.

Probe outputs: [baseline](results/experiments/baseline.allocations.json), [receive reuse](results/experiments/receive-reuse.allocations.json), [coalesced](results/experiments/coalesced.allocations.json), [combined](results/experiments/combined.allocations.json).

## Implementation priorities

1. **Batch output in the HTTP/1 driver.** Combine the head with immediately available body data, using bounded buffering or the existing scheduler `write_vectored` interface. Handle partial vectored writes and preserve backpressure. The experiment demonstrates the opportunity even though it adds a copy; it does not establish which batching implementation is fastest.
2. **Preserve streaming progress.** Flush promptly if a body-read future returns `Pending`, at explicit flush boundaries, and for interim responses and upgrade/CONNECT handshakes. `ReadableBody::is_ready()` is a hint and does not promise one-poll completion. Removing the current unconditional head flush based solely on that predicate could delay headers or deadlock an exchange. The static diagnostic patch is not a production-ready implementation.
3. **Reuse parser storage.** Retain receive capacity, then investigate reusing parser header storage and serialized-head capacity. Receive reuse has a clear allocation benefit, but allocated volume alone is a poor guide to throughput priority here.
4. **Reduce avoidable owned-field copies, then reprofile.** Framing currently clones/rebuilds the response head, and request admission clones the head for exchange state. Preserve the existing semantics while measuring each change. A specialized single-chunk static body might also remove one small allocation, but its throughput value is unmeasured.

There is no evidence here that replacing generic bodies or the semantic API should come first. Batching leaves a throughput gap to Hyper; the experiment does not explain every part of that gap. A post-batching profile should guide subsequent HTTP, I/O-binding and native-reactor work.

These findings apply to small persistent plaintext HTTP/1 responses on macOS readiness I/O. They do not evaluate Linux io_uring, multi-worker scaling, streaming/proxy workloads, TLS or HTTP/2/3. Reproduction commands are in the [benchmark readme](readme.md).

Tooling validation passed: release builds, formatting, Clippy with all targets/features and warnings denied, Python syntax checks, and an all-target/all-feature check on Rust 1.85.1. The 26 production source/manifest hashes recorded in the original baseline still match. The optional allocation/demangling dependencies are confined to the benchmark package; original baseline provenance is preserved.
