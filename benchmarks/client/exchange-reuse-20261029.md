# Direct client operation storage — 29 October 2026

The direct pooled HTTP/1 client now reuses the three large pinned operation allocations across requests on an established connection. Each exchange retains its own cancellation/completion identity and independently driven upload/download. This reduces allocation traffic; timing evidence and the remaining Hyper gap are recorded separately below.

## Ownership and implementation

The connection owns one factory with typed caches for the opening future, request sink and response stream. Each cache holds at most one **empty** allocation. A live handle exclusively owns its pinned operation. Dropping that handle first drops all captures in place, then returns empty storage through a weak cache reference. Idle storage cannot retain a pool use, interim observer, request metadata or strong connection owner. Overlapping returns can replace/discard surplus storage; they never share a live operation.

`pin-project-lite` supplies safe pinned projection and replacement. An explicit occupied/empty slot allows generated replacement to drop a `!Unpin` operation in place. `crossbeam-utils::AtomicCell` transfers the pointer-sized optional pinned box. Its take/swap operations are lock-free on this measured target. The client retains `forbid(unsafe_code)`; there is no handwritten pointer manipulation or lifetime extension. Registry scheduler/pool 0.2.0 and all existing registry dependency versions remain fixed.

The factory selects storage **before** constructing the next large operation. Transport operations retain static dispatch. Existing erased handles still poll the concrete future, sink or stream; no new body queue, application task or per-frame boxed future is introduced. Known-empty input drops its response stream immediately after returning the decoder, allowing reuse while the old response value remains alive. Terminal nonempty input similarly releases its stream after the final frame. Completion records remain per exchange so old completed writers, responses and cancellation signals cannot affect the next request.

Production changes are in [storage reuse](../../crates/client/src/reuse.rs), [writable handles/factory](../../crates/client/src/writable.rs) and [connection ownership](../../crates/client/src/connection.rs). Public HTTP semantics remain the same. Queued pull-body `call` continues to share the connection codecs and pool with direct `open`.

## Tradeoff

The connection retains up to three empty allocations while idle, plus small factory/cache state. A live operation can briefly overlap another returning allocation; surplus empty entries are discarded. This is a bound per connection, not a global storage cache or payload cache. Closed connections release their caches. Each exchange still allocates its distinct small state. Construction adds factory/cache allocations, so lower steady-state traffic does not imply fewer allocations on fresh connections. Fresh-connection throughput, Linux and multiworker scaling were not measured in this iteration.

## Rejected versions and profiling

The first mutex-backed cache removed three allocation calls but regressed the five-pair throughput median by 3.3% for HTTP and 3.0% for HTTPS. Replacing its six per-exchange lock/unlock sequences with single-slot atomic transfers recovered plaintext performance, but HTTPS remained 3.7% slower in that cohort. Both exploratory cohorts and their original sources are retained: [mutex](results/exchange-reuse-repeat-20261029/), [atomic Option](results/exchange-reuse-atomic-repeat-20261029/). Allocation-counter throughput is not used for those conclusions.

Separate three-second TLS profiles showed `_platform_memmove` in 90 baseline samples versus 165 atomic-Option samples, out of roughly 2,300 observations of the active worker per profile. These are diagnostic samples, not a portable percentage or timing result. Assembly exposed a concrete unnecessary copy: clearing a pinned `Option` dropped its sink state and then copied 6,304 bytes of inactive payload from the stack. Merely giving the slot an explicit enum tag did not remove it. Generated `project_replace` did; the inspected replacement routines contain no memcpy calls. The [profiles and assembly extracts](results/exchange-reuse-profiles-20261029/) preserve this evidence.

A subsequent [replacement cohort](results/exchange-reuse-replace-repeat-20261029/) still had significant timing variation. The final implementation also selects idle storage before constructing the next operation, avoiding eager large temporaries ahead of the cache branch. Its frozen executable and complete results are described below; exploratory runs are not counted as repetitions of that version.

## Final allocation diagnostic

A separate final build with `count-allocations` measures whole-process allocation calls and requested bytes during a one-second timed phase after 0.3 seconds of warmup, at concurrency 32. Setup/warmup are excluded; phase scaffolding and cleanup workers remain included. Small fractional counts reflect that amortized work. These bytes measure allocation **traffic**, not retained/live memory, and the counting executable is not used for throughput claims. The [raw diagnostic](results/exchange-reuse-final-allocations-20261029/) compares the preserved previous counting binary with the final implementation against the same common server.

| Transport | Client | Allocations/response, rounded | Requested bytes/response, approximately |
|---|---|---:|---:|
| HTTP | Connection-only Base | 6 | 322 |
| HTTP | Updated pooled Base | 12 | 839 |
| HTTP | Previous pooled Base | 15 | 14,766 |
| HTTP | Pooled Hyper | 15 + 1 reallocation | 12,612 |
| HTTPS | Connection-only Base | 8 | 501 |
| HTTPS | Updated pooled Base | 14 | 1,016 |
| HTTPS | Previous pooled Base | 17 | 12,673 |
| HTTPS | Pooled Hyper | 17 + 1 reallocation | 12,793 |

This removes **three allocation calls per response**, reducing calls by 20% for HTTP and 18% for HTTPS. Requested-byte traffic falls **94.3% for HTTP and 92.0% for HTTPS**. The three previously frequent multi-kilobyte allocations disappear from the steady-state histogram. All eight diagnostic samples passed exact accounting, 32 connections each and zero server failures. The remaining difference over connection-only Base is six allocation calls per response, including metadata defaults and exchange/cancellation/admission state; it is not an isolated pool measurement.

## Final throughput evidence

The final five-pair repeat uses four-second timed phases, 0.5 seconds of warmup and 32 persistent connections. Median per-pair throughput rises **1.4% for HTTP and 1.4% for HTTPS**; all five pairs improve for each transport. Median per-pair CPU/response falls about 1.3% for both. One plaintext baseline sample is unusually slow (a 22.7% pair improvement), so the aggregate medians are misleading: use the paired median, preserve every sample, and treat the speedup as modest evidence on a shared development host. Individual before/after ranges overlap substantially.

| Transport | Concurrency | Client | Median responses/s | Range | CPU µs/response |
|---|---:|---|---:|---:|---:|
| HTTP | 32 | `base-open` | 87,968 | 86,960–102,259 | 11.29 |
| HTTP | 32 | `before-open` | 86,577 | 82,960–98,280 | 11.47 |
| HTTPS | 32 | `base-open` | 83,181 | 81,729–92,550 | 11.94 |
| HTTPS | 32 | `before-open` | 81,976 | 79,232–89,489 | 12.09 |

The broader comparison uses three repetitions, two-second timed phases and 0.5 seconds of warmup, at concurrency one and 32. It uses the same frozen final executable, the same independent two-worker Hyper server and the [existing harness method](readme.md#method): an empty GET, a checked 11-byte response, one client worker, no pipelining, and matched TLS identity/ALPN/version settings. Base direct requests run in their application futures; Hyper retains its connection driver tasks. CPU includes each client's actual orchestration. There were no concurrent builds, tests or profilers during normal timing.

| Transport | Concurrency | Client | Median responses/s | Range | CPU µs/response |
|---|---:|---|---:|---:|---:|
| HTTP | 1 | `base-connection` | 38,321 | 38,205–41,365 | 12.69 |
| HTTP | 1 | `base-open` | 36,102 | 35,180–36,144 | 15.31 |
| HTTP | 1 | `before-open` | 36,199 | 33,154–36,347 | 15.63 |
| HTTP | 1 | `hyper` | 35,377 | 33,889–35,772 | 14.35 |
| HTTP | 1 | `hyper-connection` | 40,545 | 39,283–41,265 | 11.85 |
| HTTP | 32 | `base-connection` | 104,235 | 102,547–105,037 | 9.51 |
| HTTP | 32 | `base-open` | 87,886 | 86,125–88,322 | 11.31 |
| HTTP | 32 | `before-open` | 85,601 | 85,445–86,892 | 11.62 |
| HTTP | 32 | `hyper` | 94,764 | 93,099–96,085 | 10.48 |
| HTTP | 32 | `hyper-connection` | 103,703 | 100,652–104,297 | 9.59 |
| HTTPS | 1 | `base-connection` | 34,470 | 33,587–35,868 | 14.69 |
| HTTPS | 1 | `base-open` | 31,631 | 31,289–33,482 | 17.34 |
| HTTPS | 1 | `before-open` | 30,403 | 29,949–31,392 | 18.36 |
| HTTPS | 1 | `hyper` | 32,291 | 30,779–33,792 | 15.73 |
| HTTPS | 1 | `hyper-connection` | 34,333 | 32,295–35,087 | 14.16 |
| HTTPS | 32 | `base-connection` | 95,282 | 95,074–95,644 | 10.41 |
| HTTPS | 32 | `base-open` | 83,374 | 81,549–83,597 | 11.90 |
| HTTPS | 32 | `before-open` | 82,198 | 80,463–83,122 | 12.08 |
| HTTPS | 32 | `hyper` | 89,653 | 88,781–89,939 | 11.06 |
| HTTPS | 32 | `hyper-connection` | 97,064 | 96,214–97,490 | 10.21 |

At concurrency 32, pooled Base is **7.3% behind Hyper for HTTP and 7.0% for HTTPS** by median throughput in this cohort. The connection-only paths are close: Base is 0.5% ahead for HTTP and 1.8% behind for HTTPS. These small connection-only differences are within the observed run variation; they do not establish a portable win. At concurrency one, pooled Base is about 2.1% ahead for HTTP and 2.0% behind for HTTPS in throughput, while Hyper uses less client CPU. The remaining concurrent pooled gap calls for isolating admission, cancellation and operation construction costs; matching allocation count alone is insufficient.

All **80 final timing samples** passed complete response validation, exact request accounting and exact connection counts, with **14,604,098 measured responses and zero server failures**. The [audit](results/exchange-reuse-final-20261029/audit.json) also verifies all 68 archived source hashes before subsequent documentation edits. No normal result contains allocation counters. Saturation P99 values were collected at different achieved request rates and are **not equal-rate latency comparisons**. Results are specific to this shared Apple M4 Pro Mac and Rust 1.87.

Raw [repeat](results/exchange-reuse-final-repeat-20261029/summary.json), [paired analysis](results/exchange-reuse-final-repeat-20261029/paired-analysis.json), [broad comparison](results/exchange-reuse-final-20261029/summary.json) and [measured source archive](results/exchange-reuse-final-20261029/sources.tar.gz) are retained.

## Validation and audit

Rust 1.87 passes **44 client integration tests with rustls**, including HTTP/HTTPS full duplex, proxy backpressure, retained completed handles, interim cancellation, deferred framing failures, partial input abandonment and joined shutdown. The default build passes **37 integration tests**. Four unit tests verify stable addresses for `!Unpin` state and in-place destruction, clearing unpolled captures, exclusive storage while completed handles remain alive, cache lifetime without cycles, bounded surplus returns and concurrent take/return. A new integration test retains six completed writers/responses while alternating empty and chunked/trailer responses; each completed adapter releases its observer before the next exchange.

Formatting and all-target Clippy pass with warnings denied for the client with rustls and for the benchmark with allocation counting enabled/disabled. The [validation log](results/exchange-reuse-final-20261029/validation.json) retains the default-test, formatting and Clippy commands and output. Production source changes are limited to the client and its direct dependencies; HTTP semantics/codecs, endpoint, scheduler and pool sources match the previous measured snapshot.

Reproduce using the existing harness, preserving the ordinary executable before rebuilding with counters:

```sh
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
  CARGO_TARGET_DIR=/tmp/base-http-validation-target \
  rustup run 1.87.0 cargo build --manifest-path benchmarks/client/Cargo.toml --release --locked --offline
cp /tmp/base-http-validation-target/release/base-http-client-benchmark /tmp/base-http-reuse-new

python3 benchmarks/client/run.py \
  --binary /tmp/base-http-reuse-new \
  --output benchmarks/client/results/reuse-repeat-new \
  --baseline-binary /tmp/base-http-exchange-compact-20261029 \
  --baseline-sources benchmarks/client/results/exchange-compact-repeat-20261029/sources.tar.gz \
  --repeats 5 --seconds 4 --warmup 0.5 --concurrency 32 \
  --implementations before-open,base-open
```

The broad run uses `--repeats 3 --seconds 2 --warmup 0.5 --concurrency 1,32 --implementations before-open,base-open,base-connection,hyper,hyper-connection`. For diagnostics, rebuild with `--features count-allocations` and compare against `/tmp/base-http-exchange-allocation-compact-20261029` with `results/exchange-allocations-compact-20261029/sources.tar.gz` as baseline sources. Use `--repeats 1 --seconds 1 --warmup 0.3 --concurrency 32 --implementations before-open,base-open,base-connection,hyper`.

Final normal executable: `/tmp/base-http-exchange-reuse-final-20261029`, SHA-256 `d6e9658443587be61afa277065f39c962c23e28d5196581eb3d07fd091c42329`.

Final diagnostic executable: `/tmp/base-http-exchange-reuse-final-allocation-20261029`, SHA-256 `311eb3bdcf768358af08826a4db37914c8dd5519c960a83ae8f3ba9fd64a8944`.

Normal baseline: `/tmp/base-http-exchange-compact-20261029`, SHA-256 `fc18f5d5acd901dd303bf00350895711fe565db9df39efca4d077647988c0bd6`. Diagnostic baseline: `/tmp/base-http-exchange-allocation-compact-20261029`, SHA-256 `3da3e911e5fef4eec4c98c9accc2c5d1c30438fcfc7183bac61882794d94518f`.

The [validation supplement](results/exchange-reuse-final-20261029/validation-supplement.tar.gz) preserves the final tests, documentation and production review diff alongside the measured source archives and hashes. Documentation edited after timing is separate from the frozen measured snapshot.

