# Remaining HTTP/1 gap: future ownership

Investigated 2026-10-27 on macOS arm64 / Apple M4 Pro, following the [1.7% Base/Hyper repeat](http-hot-path.md). This investigation isolates cancellation-wrapper work, movement of large futures and a request-ownership difference in the benchmark handlers. The final paired repeat measures **127,566 Base versus 128,881 Hyper req/s**, a **1.0% remaining gap**. Throughput results below distinguish production changes from benchmark corrections; no Base win is established.

## What the fresh profile found

The saved baseline's application-thread wall-clock samples include 124 exclusive samples in libc memory copying out of 9,046 samples (1.4%). Many callers are request admission, handler execution and outgoing response work. Hyper has less user-space work in the same sample. Socket sends and receives still dominate both profiles. These are wall-clock stack samples, including kernel/blocking time, not an on-CPU breakdown; they motivate inspecting copies but do not assign a fraction of the throughput deficit to them. [Profiles, raw stacks and category summary](results/remaining-gap/profiles/).

Three small production changes remove work:

- The selector's cancellation wrapper constructs and pins cancellation/closure futures even with a never-cancelled token and worker-owned readiness. Neither wait contributes there: cancellation cannot fire, and direction waits already handle closure. After the same admission checks, that case can await the operation directly. Real cancellation and independently driven selectors keep the old path.
- The server passes a request-owning handler future by value through a cancellation wrapper. Output similarly passes body-read futures through its wait wrapper. Pinning these futures at construction and passing pinned references removes those additional ownership transfers. Original storage remains local and alive while suspended. The handler future is dropped before interim finalization; the read future is dropped before chunk encoding, including after the flush/resume path. No box, task, body trait or public API is introduced.
- The selector's bound reader/writer also pass their async operation by value into the cancellation helper. Pinning that operation at construction removes the transfer into the helper and its local pinned storage. The reader's inner scope ends before updating its established-readiness state. Registration, generation tracking, syscall probing, EOF and backpressure logic are unchanged.

Maximum future sizes need not shrink: an async state machine still reserves room for its largest branch. The relevant question is how much work actually executes and how much memory is moved.

## Rejected storage optimizations

Moving pending trailer fields from `IncomingBody` to its exclusively borrowed input lane shrank the body from 176 to 64 bytes and the complete request from 416 to 304 bytes, without changing the two-allocation/15-byte codec result. It moved 112 bytes into the long-lived reader rather than eliminating that storage.

The matched Rust 1.99.1 comparison nevertheless regressed median user CPU **3.7%** and throughput **1.3%**. The production change was reverted. Smaller Rust values alone were insufficient evidence of faster generated code. New server/client trailer lifecycle regressions remain useful independently of the experiment. [Comparison](results/remaining-gap/trailer-comparison/summary.json), [candidate patch and rejection](results/remaining-gap/trailers-current/).

An earlier screen accidentally mixed the Rust 1.85.1 candidate with a Rust 1.99.1 baseline. It is explicitly invalidated and excluded. During restoration, a layout probe also detected that preserving the original source mtime left a stale Cargo artifact; the source was touched and rebuilt before timing the next candidate. The stale binary was not timed. [Build provenance](results/remaining-gap/build-provenance.json), [invalid screen](results/remaining-gap/trailer-screen/invalid.json).

A second screen kept trailers in the body but boxed only nonempty completed fields, retaining explicit once-only publication for an empty trailer section. That shrank `IncomingBody` from 176 to 72 bytes and the request to 312 bytes. Exported-copy bytes fell from 2,143 to 1,623; ordinary static allocations stayed unchanged. However, median user CPU rose **2.1%**, and throughput fell **0.5%**. Four of five paired rounds used more user CPU. This change too was **reverted**: the measurements do not justify an extra allocation for nonempty trailers. The empty-trailer regression tests remain. [Screen](results/remaining-gap/boxed-comparison/summary.json), [patch and rejection](results/remaining-gap/boxed/).

## Measurements

The preserved intermediate cohort compares the original baseline, the selector shortcut alone, the shortcut plus HTTP pinning, and Hyper/Tokio. A subsequent cohort includes selector stream pinning. All use Homebrew Rust 1.99.1 / LLVM 23 and the same release profile, service, TCP options and response construction. The [runner](compare_candidates.py) uses isolated fresh processes, one application worker, four wrk threads, 64 persistent connections, one-second warmups and five interleaved eight-second samples per variant. Compilers, tests, profilers and other load tests finish before timing. Host background activity and CPU placement remain uncontrolled.

The first complete stream-pinning cohort shows **8.0% lower median user CPU/request**, but **5.3% lower median throughput than the original baseline**, and **7.9% below Hyper**. User CPU improves in every paired round, while Hyper wins all five throughput rounds. All samples are error-free. Early samples for every variant were much slower, and kernel CPU varies substantially; this is evidence of lower user-space cost, not a demonstrated throughput win. [Samples](results/remaining-gap/final-comparison/samples.jsonl), [summary](results/remaining-gap/final-comparison/summary.json).

### Matching handler ownership

Caller attribution and disassembly exposed an additional benchmark difference. Base's `async fn Hello::call` captures the unused owned request until the returned future is polled. Hyper's `service_fn(|_: Request<_>| async { ... })` drops it synchronously before returning an uncapturing future. The Base benchmark now uses the same plain-function/uncapturing-async-block structure. Both still construct fresh response fields and bodies when polled; no encoded response is cached. This is a benchmark correction using the existing `Handler` interface, not a production API change or a transformation to apply blindly to arbitrary handlers.

A separate pre-production binary was rebuilt with this corrected handler. Five source swaps restored the exact saved production baseline for compilation, then restored and hash-verified current production sources. This isolates production changes from the handler correction. The four-control cohort compares matched before/current, old-handler current and Hyper. [Source swaps and provenance](results/remaining-gap/matched-baseline/), [variant configuration](results/remaining-gap/matched-variants.json).

| Variant | Median req/s | User µs/request | System µs/request |
| --- | ---: | ---: | ---: |
| Matched handler, before | 126,100 | 0.974 | 6.931 |
| Original handler, current | 124,814 | 0.873 | 7.106 |
| Matched handler, current | 126,635 | 0.849 | 7.020 |
| Hyper/Tokio | 130,494 | 0.674 | 6.963 |

The corrected Base/Hyper comparison is **126,635 versus 130,494 req/s**, a **3.0% Hyper lead**. Production changes reduce median user CPU/request **12.9%** against the equally corrected baseline, with only **0.4% higher median throughput**. All errors are zero. One current sample falls to 100,856 req/s and a later Hyper sample to 111,276 req/s, so uncontrolled host/kernel variation is still material. These results support lower user-space cost and a remaining Hyper lead, not parity or a demonstrated production throughput win. [All samples](results/remaining-gap/matched-comparison/samples.jsonl), [summary](results/remaining-gap/matched-comparison/summary.json), [environment](results/remaining-gap/matched-comparison/environment.json).

### Final confirmation

After restoring and rebuilding the retained sources, a fresh five-round paired repeat measures **127,566 Base versus 128,881 Hyper req/s**, with Base **1.0% behind**. Base leads two rounds and Hyper three. There are no wrk errors. Base's range is 126,276–128,777; Hyper's is 114,707–130,007. In particular, the final Hyper sample slows substantially, so this repeat does not establish parity or a Base win. [Samples](results/remaining-gap/confirmation/samples.jsonl), [summary](results/remaining-gap/confirmation/summary.json), [environment](results/remaining-gap/confirmation/environment.json).

Median user CPU/request is **0.828 µs Base versus 0.680 µs Hyper**; system CPU is **6.982 versus 7.053 µs**. Remaining user-space cost is about **0.149 µs/request** in this cohort. Lower user CPU is real progress, but most CPU time is system time, and kernel/host variation can outweigh a small userspace saving. The earlier matched before/current cohort, rather than this confirmation's Hyper control, supplies the production CPU delta above.

Copy-count diagnostics interpose exported libc `memcpy`/`memmove` calls while a separate client validates exactly 10,000 sequential requests after warmup. They exclude inlined copies and kernel copies; instrumented timings are never used for throughput claims. [Counter source](results/remaining-gap/tools/memory_counts.c), [driver](results/remaining-gap/tools/count_memory.py).

| Handler / production state | Exported copy calls/request | Bytes/request |
| --- | ---: | ---: |
| Original handler, before | 26 | 4,671 |
| Original handler, cancellation shortcut | 26 | 4,671 |
| Original handler, plus HTTP callsite pinning | 24 | 3,775 |
| Original handler, plus selector stream pinning | 20 | 2,559 |
| Matched handler, before | 23 | 3,359 |
| Matched handler, current | 19 | 2,143 |
| Hyper | 4 | 24 |

For the original handler, production changes remove six exported copies and 2,112 bytes/request (**45.2%**). With matched handler ownership, they remove four copies and 1,216 bytes/request (**36.2%**); the static handler no longer carries a large request through the HTTP cancellation wrapper. The benchmark correction itself saves one 416-byte copy in the current build. These distinctions prevent attributing a harness effect to the production library. [Initial counts](results/remaining-gap/memory-counts.json), [original-stage counts](results/remaining-gap/stream-pin-memory-counts.json), [matched counts and callers](results/remaining-gap/matched-memory-counts.json).

Remaining large copies concentrate around request admission and handoff in the exchange loop. Caller line information includes inlined code and does not resolve every site; no exact source attribution is claimed for every byte. A final independent 10,000-request sequential socket diagnostic records **one receive, zero receive-WouldBlock calls and one successful write per request** for both Native and Hyper. This checks the sequential path only; instrumented timings are excluded. The Base/Tokio control still issues an extra receive probe. [Socket counts](results/remaining-gap/final-socket-counts.json).

Codec allocations remain **two allocations / 15 bytes per request**, and the complete response still uses **one write**. [Final allocation/layout probe](results/remaining-gap/final-allocation-layout.json).

## Correctness and reproduction

The changed paths preserve lazy operation construction, pre-cancellation/admission checks, partial-read state, real cancellation, shutdown wakeups, short writes, EOF reads, trailer finalization, independent duplex progress and joined retirement. Fragmented request/response tests also check that empty trailer boundaries publish exactly once, including close-before-finish and a cleared destination on repeated finish. The new future-object Drop guard checks that a completed read is destroyed before output writes a 32 KiB chunk. Existing suspended-read and uncooperative-body cancellation tests remain in place.

Rust 1.85.1 passes **45 codec tests and 28 server/documentation tests**, including all five TCP proxy scenarios. Scheduler all-target/all-feature host coverage passes **28,981/28,981 measured source regions**, with the existing three invariant-only unreachable exclusions. A deterministic test covers an event collected after routing retirement but before kernel-filter deletion; this replaces timing-dependent coverage of an existing supported race window. Clippy with warnings denied passes for the scheduler and HTTP packages. Independent implementation and duplex reviews found no correctness defect in the scoped changes. [Validation logs and preserved sources/binaries](results/remaining-gap/), [isolated task source patch](results/remaining-gap/task-source.patch), [final source hashes](results/remaining-gap/after-source-hashes.json), [machine-readable final result](results/remaining-gap/final-results.json).

Run from this directory after preserving the baseline and adjusting paths for the local checkout:

```sh
cargo build --release --locked --features profile-tools --bins
python3 compare_candidates.py --wrk /absolute/path/to/wrk \
  --variants results/remaining-gap/matched-variants.json \
  --output results/new-future-comparison --repetitions 5 --duration 8
```

The final serving binary is byte-for-byte identical to the preserved matched-current candidate after reverting the boxed-trailer screen. Both source hashes and binary identity were checked before confirmation.

These results concern persistent plaintext small-response HTTP/1 on one macOS application worker. They do not establish Linux/io_uring, TLS, streaming, connection-churn or multi-worker performance. HTTP projects have no Git baseline here; explicit before/after source snapshots define the task's comparison base, while scheduler changes are isolated from its preexisting working-tree edits.

## Remaining work

The retained current code still executes 2,143 exported-copy bytes/request and two request-string allocations in this workload. Large copies concentrate around parsing/admission results and request handoff. The rejected storage experiments show that reducing value size or copy counts alone is insufficient; the next useful work is to inspect the generated admission path and remove transfers without moving costs into extra state or allocations. Body exhaustion cannot safely skip a final read under the current semantic contract: `is_empty` does not suppress read errors. Neither pooling, TLS nor a different public body API is justified by these measurements.

The subsequent [raw read/write control](raw-io.md) removes all HTTP work and tests scheduler/I/O overhead directly, including Tokio root-versus-spawned-supervisor execution.
