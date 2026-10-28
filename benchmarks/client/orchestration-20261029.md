# Client orchestration cost — 29 October 2026

A subsequent [full comparison rerun](orchestration-full-rerun-20261029.md) using identical binaries shows wider concurrent gaps of 6.4%/11.0%. The approximately 2% Hyper gap below describes this original cohort and did not reproduce consistently.

The pooled HTTP/1 client now avoids normal-completion maintenance wakes, unnecessary cancellation subscriptions and copies of inactive future storage. The five-pair before/after repeat improves median paired throughput by **2.1% for HTTP and 1.5% for HTTPS**, with approximately matching CPU reductions. Four of five pairs improve for each transport. Ranges overlap on this shared development host; these are modest improvements, not a portable speedup guarantee.

## Cost model and implementation

An established direct exchange acquires a pool use, takes the retained connection codecs, writes the request, consumes the response and returns both codecs before completing the use. Its application future owns progress. Background work is needed for construction, queued calls, deferred closure and retirement. Normal reuse should not manufacture background work.

The isolated pool benchmark measures completion, reacquisition and activation of capacity-one resources, with one or 32 occupied registrations. It performs no HTTP, socket I/O, exchange construction or erased body polling. The optional retirement observer is explicitly repolled after each cycle. This makes its cost useful for isolation, rather than an exact model of HTTP task scheduling.

| Resources | Retirement observer | Before CPU µs/cycle | After CPU µs/cycle |
|---|---|---:|---:|
| 1 | No | 0.1074 | 0.0842 |
| 1 | Yes | 0.1578 | 0.0953 |
| 32 | No | 0.1557 | 0.1267 |
| 32 | Yes | 0.2200 | 0.1388 |

The retirement observer receives **one wake per cycle before and zero afterward**. A separate diagnostic records **zero pool allocations per warm cycle**. Timer and loop overhead are included; these costs should not be subtracted directly from HTTP timings. The initial idea that removing and reinserting the sole B-tree entry allocated another leaf was wrong: Rust retains the empty root leaf. Updating that entry in place saves operations, not allocations. Pool admission alone is much smaller than the previously measured 1.5–1.9 µs difference between the full client and connection-only paths.

The changes follow that ownership model:

- **Pool notifications follow predicates.** Retirement has a separate event. Queueing/requeueing cleanup, retirement acknowledgment, shutdown and abandoned construction notify it. Ordinary reservations, activation, reuse and admission updates do not. Immediate acquisition with no queued waiters does not notify; queued acquisition still wakes successors. Successful activation updates Reserved to Active in place. Rejected activation still releases its reservation and notifies.
- **Accepted-request accounting wakes supervision only for closure.** Dropping a pending request wakes the client supervisor when a closed client reaches zero pending exchanges. Submission and deferred cleanup retain their own notifications. Admission remains a synchronous Busy/Closed check.
- **Cancellation subscribes only when work waits.** A concrete pinned `Checked` future checks cancellation first, polls its nested work directly, then polls the cancellation future if work returns Pending. That final poll registers a listener and observes cancellation racing the first check. Already-cancelled operations never start. This also removes the async wrapper's first-poll move into a second pinned storage slot.
- **Pinned transitions drop and replace live state directly.** Both cache initialization and the private sequential output sink use generated `project_replace`. The former `Pin::set` transitions could copy inactive future payloads. `OwnedSink` retains the existing sequential Sink boundary and does not allocate per frame. Pending writes remain pinned through completion or cancellation.

The public client/body interfaces, duplex ownership, per-exchange cancellation identities, completion accounting and transport dispatch are unchanged. No additional application task or body-data queue is introduced. Old completed handles still cannot cancel a later exchange. Both directions return before admission is released, and explicit cleanup remains joined.

Production changes are in the [client driver](../../crates/client/src/driver.rs), [storage cache](../../crates/client/src/reuse.rs), [sequential sink](../../crates/client/src/sink.rs), [pool core](../../../base-pool/src/core.rs), [activation](../../../base-pool/src/multiplex.rs) and [retirement queue](../../../base-pool/src/retirement.rs).

## Assembly and allocation evidence

The [retained assembly extracts](results/orchestration-assembly-20261029/) show approximate 7 KiB HTTP and 6 KiB HTTPS inactive-payload copies disappearing from cache initialization. The sequential sink removes the analogous large copy when restoring its small codec owner. Storing nested cancellation work directly removes additional approximately 3.5 KiB/2.9 KiB first-poll copies from the inspected sink routines. The final HTTP sink completion routine has only 288-byte codec-owner memcpy calls; the TLS variant has none. These are specific inspected routines on Rust 1.87/arm64, not a claim that the entire request path is copy-free or a sum of bytes executed per request.

The [allocation diagnostic](results/orchestration-final-allocations-20261029/) remains approximately **12 allocations/response for HTTP and 14 for HTTPS**, the same as before. Connection-only Base remains 6/8. Requested-byte traffic is also unchanged. The improvements target work within retained storage and notifications. Counter-enabled throughput is not used for timing conclusions.

## Before/after repeat

Five alternating pairs per transport use 32 persistent connections, four-second timed phases and 0.5 seconds of warmup. The frozen previous implementation is compared against the final implementation through the same common independent Hyper server. Response bodies and server request/connection accounting are checked for every sample. No builds, tests, profilers or assembly extraction run alongside this final timing cohort.

| Transport | Median per-pair throughput change | Median per-pair CPU change | Improving pairs |
|---|---:|---:|---:|
| HTTP | +2.15% | −2.12% | 4/5 |
| HTTPS | +1.54% | −1.52% | 4/5 |

HTTP pairs range from −5.2% to +8.0%; HTTPS from −1.6% to +5.7%. The aggregate HTTP throughput medians are 110,149 before and 114,284 after. HTTPS aggregate medians are 101,940 before and 101,457 after, despite four of five local pairs improving. Changing host speed over the cohort makes those aggregate medians misleading as an improvement estimate; both raw aggregates and paired results are retained. This is evidence of a small benefit with material run variation.

Raw [repeat summary](results/orchestration-final-repeat-20261029/summary.json), [paired analysis](results/orchestration-final-repeat-20261029/paired-analysis.json) and [measured sources](results/orchestration-final-repeat-20261029/sources.tar.gz) are retained. Exploratory [component ablation](results/orchestration-ablation-20261029/) and [cumulative stages](results/orchestration-stage-repeat-20261029/) are separate; their method notes identify a contaminated exploratory sample in each. They are not included in the final before/after or Hyper comparison conclusions.

## Full comparison with Hyper

The full run uses three repetitions, two-second timed phases and 0.5 seconds of warmup, at concurrency one and 32. It uses the same frozen final executable, an independent two-worker Hyper server, an empty GET, a validated 11-byte response, one client worker and no pipelining. TLS identity, ALPN, version and resumption settings match. No builds, tests, profilers or assembly extraction run alongside this timing cohort.

| Transport | Concurrency | Client | Median responses/s | CPU µs/response |
|---|---:|---|---:|---:|
| HTTP | 1 | Previous Base pooled | 32,446 | 16.809 |
| HTTP | 1 | Updated Base pooled | 33,048 | 16.058 |
| HTTP | 1 | Hyper pooled | 31,222 | 15.967 |
| HTTP | 1 | Base connection-only | 35,947 | 14.087 |
| HTTP | 1 | Hyper connection-only | 40,374 | 11.974 |
| HTTP | 32 | Previous Base pooled | 114,068 | 8.750 |
| HTTP | 32 | Updated Base pooled | 116,873 | 8.540 |
| HTTP | 32 | Hyper pooled | 119,370 | 8.358 |
| HTTP | 32 | Base connection-only | 139,376 | 7.129 |
| HTTP | 32 | Hyper connection-only | 133,715 | 7.461 |
| HTTPS | 1 | Previous Base pooled | 31,117 | 17.466 |
| HTTPS | 1 | Updated Base pooled | 31,404 | 16.923 |
| HTTPS | 1 | Hyper pooled | 30,521 | 17.186 |
| HTTPS | 1 | Base connection-only | 33,107 | 14.921 |
| HTTPS | 1 | Hyper connection-only | 31,922 | 15.384 |
| HTTPS | 32 | Previous Base pooled | 105,074 | 9.490 |
| HTTPS | 32 | Updated Base pooled | 107,420 | 9.291 |
| HTTPS | 32 | Hyper pooled | 109,901 | 9.078 |
| HTTPS | 32 | Base connection-only | 123,620 | 8.048 |
| HTTPS | 32 | Hyper connection-only | 120,359 | 8.291 |

Updated pooled Base is **2.1% behind Hyper for HTTP and 2.3% for HTTPS** at concurrency 32 by median throughput in this cohort. Connection-only Base is 4.2%/2.7% ahead, though ranges overlap. At concurrency one, pooled Base has higher median throughput for both transports; HTTP client CPU remains slightly higher than Hyper. These comparisons describe this workload and host, not a universal ordering.

The difference between connection-only and pooled CPU is approximately **1.41 µs for HTTP and 1.24 µs for HTTPS** in Base, versus 0.90/0.79 µs in Hyper. These differences include metadata defaults, cancellation identities, ownership transfer, adapters, admission and orchestration; they are not an isolated pool cost. That remaining client layer, particularly per-exchange construction, is the next investigation target. Reusing old cancellation/completion identities would break isolation and is not an acceptable shortcut.

All **80 final timing samples**, comprising **17,434,573 measured responses**, passed body validation, exact request/connection accounting and zero server failures. The eight allocation samples also passed. The [audit](results/orchestration-final-full-20261029/audit.json) verifies all 76 measured source hashes before ordinary lockfile restoration. Raw [full comparison](results/orchestration-final-full-20261029/summary.json), [metadata](results/orchestration-final-full-20261029/metadata.json) and [source archive](results/orchestration-final-full-20261029/sources.tar.gz) retain the ranges and provenance.

## Validation and dependency status

Rust 1.87 passes **44 integration tests with rustls and eight client unit tests**. The default configuration passes 37 integration tests and eight unit tests. Regressions cover HTTP/HTTPS duplex operation, backpressure, proxy forwarding, dropped writes, interim cancellation, deferred closure, retained old handles and joined shutdown. New unit tests exercise cancellation priority and races, blocked-work wakes, supervisor wake predicates, pinned write addresses and exactly-once owner destruction on success/error/drop.

The pool passes 27 behavior tests, one TCP integration test and two doctests. New wake-count tests cover normal reuse remaining idle, actual retirement delivery, requeue, final acknowledgment, empty shutdown and the final dropped constructor. Client, pool and benchmark Clippy checks pass with warnings denied; the benchmark is checked with allocation counting enabled and disabled. [Initial validation](results/orchestration-validation-20261029/) and [final client validation](results/orchestration-final-validation-20261029/) preserve commands and logs.

The final client also passes all eight unit tests and 44 integration tests against the **published pool with no Cargo patch**, using `--locked --offline`. Its [validation log](results/orchestration-final-validation-20261029/published-pool.log) confirms that the client changes remain compatible with the released dependency. The [validation supplement](results/orchestration-final-full-20261029/validation-supplement.tar.gz) preserves tests, documentation and a review patch separately from the frozen measured sources.

**Measurements use a controlled local Cargo patch for base-pool 0.2.0.** The published dependency and registry source are unchanged. The pool changes need a release before ordinary registry-dependent client builds include them. The client improvements themselves require no dependency changes. Scheduler 0.2.0, endpoint and HTTP codec sources remain fixed. Final source archives include the patched pool and the exact benchmark lockfile; ordinary project lockfiles are restored afterward.

These results cover small-response loopback HTTP/1 on a shared Apple M4 Pro Mac. Linux epoll/io_uring, fresh TLS connections, multiworker scaling and larger streaming bodies were not timed here. Saturation P99 at different achieved rates is not an equal-rate latency comparison.

## Reproduce

Build with the explicit local pool patch, preserving the ordinary executable before enabling allocation counters:

```sh
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/base-http-validation-target \
  rustup run 1.87.0 cargo build --manifest-path benchmarks/client/Cargo.toml --release \
  --config 'patch.crates-io.base-pool.path="/Users/samuel/Developer/base-rs/base-pool"'
cp /tmp/base-http-validation-target/release/base-http-client-benchmark /tmp/base-http-orchestration-new

python3 benchmarks/client/run.py \
  --binary /tmp/base-http-orchestration-new \
  --output benchmarks/client/results/orchestration-repeat-new \
  --cargo-config 'patch.crates-io.base-pool.path="/Users/samuel/Developer/base-rs/base-pool"' \
  --baseline-binary /tmp/base-http-exchange-reuse-final-20261029 \
  --baseline-sources benchmarks/client/results/exchange-reuse-final-20261029/sources.tar.gz \
  --repeats 5 --seconds 4 --warmup 0.5 --concurrency 32 \
  --implementations before-open,base-open
```

The full comparison uses `--repeats 3 --seconds 2 --concurrency 1,32 --implementations before-open,base-open,base-connection,hyper,hyper-connection`. The isolated pool role is `binary pool --implementation pool-observed --concurrency 32 --seconds 2 --warmup 0.3`, or `--implementation pool` without an observer. Use the normal executable for timing and `--features count-allocations` builds only for allocation diagnostics.

Final normal executable: `/tmp/base-http-orchestration-final-20261029`, SHA-256 `66fe13a6e9305f41fac8bf21cf20653c461ffed8b4b366d24ce8e8c378ba2dd2`.

Final diagnostic executable: `/tmp/base-http-orchestration-final-allocation-20261029`, SHA-256 `9b204c1b2f04342f1cd44724e19c93feb933203be703f83e7cd8967ee79fd6c7`.

Normal baseline: `/tmp/base-http-exchange-reuse-final-20261029`, SHA-256 `d6e9658443587be61afa277065f39c962c23e28d5196581eb3d07fd091c42329`. Diagnostic baseline: `/tmp/base-http-exchange-reuse-final-allocation-20261029`, SHA-256 `311eb3bdcf768358af08826a4db37914c8dd5519c960a83ae8f3ba9fd64a8944`.
