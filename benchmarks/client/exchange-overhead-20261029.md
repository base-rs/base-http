# Direct exchange allocations — 29 October 2026

The direct pooled HTTP/1 client now performs **four fewer allocations per response**: HTTP falls from 19 to 15 and HTTPS from 21 to 17. Its public API and full-duplex ownership model are unchanged. Repeated timing suggests a small throughput improvement; it does not close the remaining Hyper gap.

The five-pair repeat has a median per-pair throughput change of **+2.3% for HTTP and +1.8% for HTTPS**. Allocation counts provide stronger evidence than the small timing differences on this shared development Mac.

## Implementation

- `PendingResponse` polls the existing pinned response stream for metadata and moves that same stream into the response body. The extra boxed wrapper future is gone. Dropping a pending response still cancels unfinished input.
- The existing shared exchange completion record now also owns output closure mode, the finalization result and its `AtomicWaker`. Ordinary directional completion modes live inline in their guards. Two flag allocations and the unconditional output receipt channel are removed.
- Completion waits register before checking their result, preserving racing notifications. A drop guard unregisters the waiting task on completion, cancellation or a dropped flush future. Cancelling one wait does not consume the final result. Deferred framing failures remain observable through subsequent flush/finish calls.
- Directional codecs return to their connection before admission is released. Lifecycle notifications and output completion wakes run outside the completion-state mutex. Both directions must finish before reuse; an early response does not finish an upload.
- Completion records remain distinct per exchange. They are not reset for another request while old handles can still exist. Aborting an old completed writer or cancelling its old signal cannot affect a later request on that connection.

The production changes are in [writable client handles](../../crates/client/src/writable.rs) and [connection return guards](../../crates/client/src/connection.rs). Transport operations remain statically dispatched and there is no boxed future allocation per frame. No scheduler, endpoint, pool, HTTP semantic or codec source changed in this iteration. Registry scheduler/pool 0.2.0 remain in use.

## Allocation evidence

The harness has an optional `count-allocations` feature forwarding to the system allocator while counting allocation/reallocation calls, requested bytes, frees and frequent exact allocation sizes up to 32 KiB. Counters cover the whole client process during measurement, including phase scaffolding and cleanup workers. Connection setup and warmup are excluded. Requested bytes are allocation traffic, not retained/live memory. A diagnostic run includes atomic-counter overhead and must not be used for throughput claims. The feature is disabled in every timing result below.

The [final diagnostic](results/exchange-allocations-compact-20261029/) compares the preserved original and updated executables against the same independent server, at concurrency 32. These are one-second diagnostics after 0.3 seconds of warmup; response contents and server request/connection accounting are validated as in normal runs. Small fractional allocation counts reflect phase overhead amortized over responses.

| Transport | Client | Allocations/response, rounded | Requested bytes/response, approximately |
|---|---|---:|---:|
| HTTP | Previous pooled Base | 19 | 15,200 |
| HTTP | Updated pooled Base | 15 | 14,774 |
| HTTP | Connection-only Base | 6 | 324 |
| HTTP | Pooled Hyper | 15 + 1 reallocation | 12,615 |
| HTTPS | Previous pooled Base | 21 | 13,103 |
| HTTPS | Updated pooled Base | 17 | 12,681 |
| HTTPS | Connection-only Base | 8 | 504 |
| HTTPS | Pooled Hyper | 17 + 1 reallocation | 12,800 |

That removes roughly **420 requested bytes per response**, with 21% fewer allocation calls for HTTP and 19% fewer for HTTPS. The measured client-layer allocation difference over the connection-only path falls from 13 to nine. That difference includes metadata defaults, cancellation and pool/API orchestration; it is not an isolated pool cost.

Matching Hyper's allocation count does not mean matching its cost. Three large allocations still occur once per response: 7,616 / 3,352 / 2,960 bytes for HTTP, and 6,312 / 2,776 / 2,568 bytes for HTTPS. These retained operation/adapter allocations dominate Base's requested bytes. The next experiment should focus on their layout and lifetime, rather than treating each small allocation as equally valuable. Reusing adapter storage must preserve isolation from old handles and independently owned upload/download.

## Throughput repeat

The initial broad comparison showed substantial variability even within an unchanged executable: updated concurrent HTTP ranged from 66k to 106k responses/s, and the original from 65k to 103k. We retained all samples and followed with a tighter alternating comparison. This variability prevents a precise improvement estimate from the broad cohort alone; no slow samples are discarded.

The repeat uses the frozen final binary, five repetitions, four seconds of measurement and 0.5 seconds of warmup, with 32 persistent connections. Order alternates between the original and updated clients. Each sample uses the same independent Hyper server implementation and verified self-signed TLS identity. Both clients use one worker, real loopback HTTP/1 with no pipelining, an empty GET and an 11-byte response. See the [harness method](readme.md#method) for task placement and configuration.

| Transport | Concurrency | Client | Median responses/s | Responses/s range | Client CPU µs/response |
|---|---:|---|---:|---:|---:|
| HTTP | 32 | `base-open` | 110,960 | 107,033–111,586 | 8.99 |
| HTTP | 32 | `before-open` | 106,128 | 93,740–109,718 | 9.40 |
| HTTPS | 32 | `base-open` | 101,768 | 85,018–101,843 | 9.81 |
| HTTPS | 32 | `before-open` | 99,015 | 93,420–101,015 | 10.08 |

The ratio of aggregate medians is **+4.6% for HTTP and +2.8% for HTTPS**; median client CPU falls from 9.40 to 8.99 µs for HTTP and 10.08 to 9.81 µs for HTTPS. The [paired analysis](results/exchange-compact-repeat-20261029/paired-analysis.json) is more conservative: median per-pair improvements are +2.3% and +1.8%. All five HTTP pairs improve; four of five HTTPS pairs improve, with one approximately 9% regression. Ranges overlap. Treat this as evidence of a modest improvement with meaningful host/run variation, not a precise portable speedup.

All **20 repeat samples** passed response validation and exact accounting: **8,227,897 measured responses**, zero server failures, exactly 32 connections per sample. There are no allocation counters in this binary. Measurements are on a shared Apple M4 Pro Mac with Rust 1.87. They do not measure Linux epoll/io_uring, multiworker scaling or equal-request-rate P99 latency.

## Broad comparison with Hyper

The preceding 60-sample cohort used three repetitions, two seconds of measurement and 0.5 seconds of warmup, at concurrency one and 32. All **8,226,328 measured responses** passed validation and exact accounting, with zero server failures and exactly the specified connection count. These are the same frozen final executable and source as the repeat.

The wide ranges limit conclusions, especially concurrent pooled HTTP. Single-connection pooled HTTP remains close in throughput; HTTPS connection-only remains competitive. Concurrent high-level Base still trails pooled Hyper. Use the repeat above for the before/after estimate, and retain this table to show the variability and remaining gap.

| Transport | Concurrency | Client | Median responses/s | Responses/s range | Client CPU µs/response |
|---|---:|---|---:|---:|---:|
| HTTP | 1 | `base-connection` | 40,323 | 39,669–40,903 | 12.06 |
| HTTP | 1 | `base-open` | 34,559 | 34,342–34,971 | 15.99 |
| HTTP | 1 | `before-open` | 34,988 | 34,590–35,660 | 15.72 |
| HTTP | 1 | `hyper` | 33,428 | 32,958–33,885 | 15.00 |
| HTTP | 1 | `hyper-connection` | 41,607 | 41,516–42,956 | 11.61 |
| HTTP | 32 | `base-connection` | 121,856 | 114,488–127,557 | 8.17 |
| HTTP | 32 | `base-open` | 68,674 | 66,006–105,730 | 12.38 |
| HTTP | 32 | `before-open` | 100,921 | 65,093–102,655 | 9.89 |
| HTTP | 32 | `hyper` | 110,549 | 93,917–117,714 | 9.01 |
| HTTP | 32 | `hyper-connection` | 127,210 | 100,586–128,612 | 7.84 |
| HTTPS | 1 | `base-connection` | 35,265 | 34,673–36,813 | 14.04 |
| HTTPS | 1 | `base-open` | 31,964 | 31,727–34,068 | 17.11 |
| HTTPS | 1 | `before-open` | 31,899 | 31,747–31,934 | 17.37 |
| HTTPS | 1 | `hyper` | 33,624 | 31,544–33,657 | 15.47 |
| HTTPS | 1 | `hyper-connection` | 33,192 | 32,497–34,930 | 14.71 |
| HTTPS | 32 | `base-connection` | 118,096 | 96,121–119,429 | 8.43 |
| HTTPS | 32 | `base-open` | 96,530 | 80,725–97,966 | 10.34 |
| HTTPS | 32 | `before-open` | 95,009 | 86,297–96,259 | 10.50 |
| HTTPS | 32 | `hyper` | 101,827 | 89,543–103,571 | 9.80 |
| HTTPS | 32 | `hyper-connection` | 112,008 | 92,860–118,655 | 8.91 |

## Layout experiments

The first pending-wrapper trial removed an allocation without showing a clear throughput gain. Initially, moving its local cancellation waiter into the response stream grew one allocation from 2,976 to 4,800 bytes and increased total allocation traffic despite reducing the count. The allocator size histogram exposed this.

Socket reads and `InterimResponses` already observe the local exchange signal, including cancellation of an uncooperative interim callback. Removing the duplicate waiter restores the smaller stream layout while keeping the separate caller-lifetime wrapper. A new regression aborts output while the peer's response is stuck in an observer which never completes; response cancellation still wakes and finishes correctly.

Factoring adapter construction into a synchronous helper made no useful layout improvement and was reverted. The [initial combined](results/exchange-layout-combined-20261029/), [helper extraction](results/exchange-allocations-final-20261029/) and [compact final](results/exchange-allocations-compact-20261029/) diagnostics retain their respective sources. Exploratory cohorts are not additional repetitions of the final timing result.

## Validation

Rust 1.87 passes **43 client integration tests with rustls enabled**, including HTTP/HTTPS full duplex and proxy backpressure. The default configuration passes **36 tests**. Four added regressions cover:

- Dropped and cancelled deferred-close waits followed by successful flush/finish, without releasing admission before input finishes.
- Deferred fixed-length framing failure, retained error observation and replacement of the failed connection.
- Old completed handles and cancellation signals while the next request is actively using the same connection.
- Aborting output while an interim observer never completes, waking response cancellation and allowing replacement.

Client Clippy passes for all targets with rustls enabled and warnings denied. Benchmark Clippy passes for all targets with allocation counting enabled and disabled, with warnings denied. Formatting passes. Source/tests/documentation and a review diff are preserved in the validation supplement linked below.

## Reproduce and audit

Build ordinary throughput and diagnostic executables separately, preserving each before another build:

```sh
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/base-http-validation-target \
  rustup run 1.87.0 cargo build --manifest-path benchmarks/client/Cargo.toml --release --locked --offline
cp /tmp/base-http-validation-target/release/base-http-client-benchmark /tmp/base-http-exchange-compact-20261029

python3 benchmarks/client/run.py \
  --binary /tmp/base-http-exchange-compact-20261029 \
  --output benchmarks/client/results/exchange-repeat-new \
  --baseline-binary /tmp/base-http-tls-demand-io-20261028 \
  --baseline-sources benchmarks/client/results/tls-demand-io-20261028/sources.tar.gz \
  --repeats 5 --seconds 4 --warmup 0.5 --concurrency 32 \
  --implementations before-open,base-open

CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/base-http-validation-target \
  rustup run 1.87.0 cargo build --manifest-path benchmarks/client/Cargo.toml --release --locked --offline --features count-allocations
```

To repeat the broad comparison, use three repetitions, two seconds and `--concurrency 1,32 --implementations before-open,base-open,base-connection,hyper,hyper-connection`. Allocation diagnostics use the same runner; only binaries built with `count-allocations` emit `allocation_diagnostic` in raw JSON. Compare counting binaries with other counting binaries. Diagnostic raw throughput is not performance evidence.

Raw [repeat summary](results/exchange-compact-repeat-20261029/summary.json), [repeat metadata](results/exchange-compact-repeat-20261029/metadata.json), [broad summary](results/exchange-compact-20261029/summary.json), [measured sources](results/exchange-compact-repeat-20261029/sources.tar.gz) and [source hashes](results/exchange-compact-repeat-20261029/source-hashes.json) are retained. All 67 measured source hashes matched after measurement, before documentation-only benchmark-readme edits. The [validation supplement](results/exchange-compact-repeat-20261029/validation-supplement.tar.gz) preserves integration tests, subsequent documentation and the production review diff.

Final normal executable: `/tmp/base-http-exchange-compact-20261029`, SHA-256 `fc18f5d5acd901dd303bf00350895711fe565db9df39efca4d077647988c0bd6`.

Final diagnostic executable: `/tmp/base-http-exchange-allocation-compact-20261029`, SHA-256 `3da3e911e5fef4eec4c98c9accc2c5d1c30438fcfc7183bac61882794d94518f`.

Normal baseline: `/tmp/base-http-tls-demand-io-20261028`, SHA-256 `a04dc438fe8443dab9cf2e37e9e1aeb4fd2fe98bac6f2638740b3a5dde606faf`.

The subsequent [storage reuse investigation](exchange-reuse-20261029.md) reuses the three remaining large operation allocations and records the memory-traffic reduction, rejected cache variants and remaining throughput gap.
