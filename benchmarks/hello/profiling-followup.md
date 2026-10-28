# Closing the remaining HTTP/1 performance gap

The [latest production cost-reduction report](hyper-target.md) implements the compact exchange and additional allocation/readiness changes discussed here. The experiments below retain their historical results and limitations.

Follow-up to the [initial profile](profiling.md), on the same macOS/M4 Pro host. **A worker-owned readiness driver brings native Base close to Base/Tokio. Hyper remains faster.** The remaining measured CPU difference is mostly in user code, including message ownership, framing, and scheduler/I/O bindings.

These historical measurements used diagnostic copies, with production sources unchanged at the time. Worker-owned readiness is now implemented in the [local scheduler](scheduler.md), and general HTTP batching/allocation changes are integrated in the [production HTTP comparison](http-optimization.md). Neither protocol validation nor per-request semantic request/response construction was removed, and no encoded response was cached. The final body treatment stores its single replayable chunk inline, matching the allocation-free single-chunk shape of Hyper's `Full<Bytes>`.

## Longer interleaved comparison

Five eight-second measurements per combination, each with one-second warmup and a fresh server, 64 persistent connections, four independent wrk threads, and one application execution thread. Jobs were shuffled within each repetition. All builds finished before timing.

| Treatment | Median req/s | Min–max | p99 | Process CPU |
| --- | ---: | ---: | ---: | ---: |
| Hyper/Tokio control | 131,463 | 129,605–132,653 | 0.694 ms | 99.4% |
| Base HTTP/Tokio, reduced HTTP overhead | 120,127 | 118,008–120,575 | 0.751 ms | 99.5% |
| Base HTTP/native, worker-owned readiness | 118,328 | 117,844–119,932 | 0.756 ms | 99.5% |
| Native driver with shared exchange snapshot, before final HTTP reductions | 117,489 | 115,899–118,092 | 0.763 ms | 99.5% |
| Native with background readiness and revised wait loop | 93,342 | 92,659–94,308 | 1.570 ms | 127.7% |

The native prototype is within 1.5% of Base/Tokio, but is **10.0% below Hyper** in this run. Base/Tokio is 8.6% below Hyper. This demonstrates a route to eliminating most of the native scheduling gap; it does not establish that the current stack meets or exceeds Hyper.

The last row includes the revised native wait-loop treatment, which also changed optimistic wake handling and worsened p99. It is not a production baseline or a recommendation to ship that loop. An earlier matched screening compared the same HTTP/output treatment on the background driver and owned driver: 93,500 versus 114,236 req/s, at 128.0% versus 99.3% process CPU. Base/Tokio in that screening reached 113,678 req/s. This is the stronger isolation of driver placement/readiness handling from HTTP changes.

See [long-run samples](results/final-comparison/samples.jsonl), [summary](results/final-comparison/summary.json), [environment](results/final-comparison/environment.json), [patches and source/binary hashes](results/final-comparison/sources.json), and the earlier [driver comparison](results/owned-screen/summary.json).

## What the experiments found

**Readiness ownership matters.** The released native readiness selector uses async-io's background reactor, with worker wakeups crossing threads. A foreground async-io driver improved screening throughput, while merely moving the same background I/O to a foreground executor did little. The [owned-driver prototype](experiments/owned_reactor.rs) uses the existing `NativeScheduler::with_drivers` interface: persistent edge readiness, cached read/write readiness, and event delivery on the task worker. It removes much of the extra process CPU and makes native throughput similar to the Tokio binding.

This prototype is deliberately limited to one worker, one waiter per direction, and socket operations/retirement on that worker. It retains registered sockets until explicit deregistration, and never reuses event keys. It needs bounded/generational registration storage, migration/root-operation routing, multi-waiter behavior, and cancellation/shutdown/error conformance before becoming a production selector. The throughput gain combines readiness caching, event registration behavior, and worker placement; it does not isolate their individual contributions.

**The send syscall also matters on this host.** After coalescing, Base used `sendto` while Hyper used `writev`. A diagnostic macOS `writev` path, with `SO_NOSIGPIPE` configured on stream-writer construction, reached about 118,360 req/s on Base/Tokio in a two-run screening. That supports testing the scheduler's borrowed/vectored output paths directly. It is not evidence that `writev` always wins, and the specialized patch is not a complete replacement for every socket operation/flag combination.

**Exchange cloning was unnecessarily deep.** `Exchange` originally owned a `RequestHead`. Cloning the exchange for `RequestReader::previous` and `IncomingBody` cloned names, values, authority, path and method. Sharing an immutable head reduced the codec probe from 36 allocations to 25. Combining the snapshot and atomic exchange state into one `Arc<Context>` removed another allocation. The application request still owns a separate head, preserving its ability to mutate it without rewriting the framing snapshot.

**Additional HTTP changes reduce work, but have smaller throughput effects.** The experiments avoid first cloning and then rebuilding response fields, reuse output capacity, use httparse's safe uninitialized-header parsing interface, recognize common method constants without allocating, and store one replayable body chunk inline. These preserve the generic body/handler interfaces. A final [direct serialization treatment](experiments/direct_head.rs) emits implicit Content-Length into the wire buffer for ordinary fixed-length 200 responses, with the existing general planner as fallback. Body length/error checks and connection-close behavior remain in the driver.

That last treatment reduced codec allocations from 21 to 15, but only slightly changed native median throughput: 118,124 to 118,645 req/s in its own five-run comparison. Base/Tokio reached 121,062 and Hyper 126,528 req/s. Control throughput varied widely (105,234–130,476), so this comparison does **not** prove the last few percent have been closed. [Raw results and patches](results/direct-head-comparison/summary.json) are retained separately from the more stable eight-second comparison above.

The original coalescing patch still assumes the known static workload. A production implementation must bound buffering and flush if a body-read future suspends, and preserve interim responses and upgrade/CONNECT handshakes. None of these measurements justify weakening those semantics.

## Remaining CPU cost

Uninstrumented five-second CPU-accounting samples after warmup, using macOS `proc_pid_rusage` and the host's Mach timebase conversion:

| Final direct-serialization treatment | User CPU/request | Kernel CPU/request | Total CPU/request |
| --- | ---: | ---: | ---: |
| Base/native owned driver | 1.268 µs | 7.033 µs | 8.301 µs |
| Base/Tokio | 1.153 µs | 7.093 µs | 8.246 µs |
| Hyper/Tokio | 0.679 µs | 6.917 µs | 7.596 µs |

These are process-wide CPU-time deltas, including helper work, divided by timed wrk requests. They are single explanatory samples, not another interleaved throughput result. The helper validates its clock conversion against `getrusage`. Native's remaining difference is about **0.59 µs of user CPU** and **0.12 µs of kernel CPU per request**. Reducing user work is now the clearer target. [Accounting records](results/cpu-accounting/base-owned.json), [Tokio](results/cpu-accounting/base-tokio.json), [Hyper](results/cpu-accounting/hyper-tokio.json), and [accounting helper](cpu_usage.py) are retained.

Final application-thread stack samples agree directionally: allocation/free fell to 1.9–2.1% for Base, versus 0.7% for Hyper, and Base still has more user/synchronization work. Socket syscalls remain the largest wall-clock categories. These are **wall-clock samples, not on-CPU percentages**; they cannot assign the 0.59 µs to individual functions. See [final category/symbol summary](results/final-profiles/summary.json), raw/demangled captures in the same directory, and the [post-coalescing profile](results/optimized-profiles/summary.json).

## What to implement next

1. Implement general bounded output batching and worker-owned readiness through the existing driver interface. Validate their streaming and lifecycle behavior before replacing the diagnostic paths.
2. Eliminate the remaining full request-head snapshot copy. The 15-allocation final probe can be accounted for by one raw-head copy, five allocations for the parsed request, five more for the framing snapshot, one shared context, and three for response fields. Sharing the exchange context removed repeated copies, but the initial request-to-snapshot copy remains. Shared immutable backing or copy-on-write storage can preserve independent mutation while avoiding it; this needs a deliberate ownership/API design.
3. Reduce per-field/name/value allocation and keep framing metadata separate from application fields during serialization. Reuse parsed input storage where lifetimes permit. The aim is to approach Hyper's roughly 0.68 µs of user CPU per request, compared with native Base's 1.27 µs in the accounting sample.
4. Reprofile each change against a fresh, interleaved control. The residual gap is small enough that host variation and five-second outliers can hide or mimic progress. Then repeat on Linux with the actual io_uring driver; this macOS experiment says nothing about io_uring throughput.

Generic bodies and the semantic behaviors can remain. The evidence points to storage ownership, serialization and driver placement, rather than a need to remove interim responses, trailers, upgrades or full duplex.

## Reproduction and validation

`experiment_next.py` records each staged patch, source/lockfile/binary hash, warmup, independent preflight, stderr and wrk output. Registry package versions/checksums are checked against the existing benchmark lockfile; local selector/Tokio copies are explicit patches. Early two-run screens are retained under `results/next-screen` and `results/writev-screen`; they select candidates rather than establish final gains.

```sh
python3 experiment_next.py --wrk /absolute/path/to/wrk \
  --output results/new-comparison --duration 8 --repetitions 5 \
  --jobs lean:base-owned lean:base-tokio shared-exchange:base-owned \
         writev:base-native combined:hyper-tokio
python3 profile.py --wrk /absolute/path/to/wrk \
  --binary target/next-experiments/direct-head/server \
  --output results/new-profiles --backends base-owned base-tokio hyper-tokio
python3 summarize_profile.py --directory results/new-profiles \
  --backends base-owned base-tokio hyper-tokio
python3 profile.py --wrk /absolute/path/to/wrk \
  --binary target/next-experiments/direct-head/server \
  --output results/new-cpu-accounting --backends base-owned base-tokio hyper-tokio \
  --no-sample --seconds 0
python3 validate_http.py
```

Build `direct-head` first using `python3 experiment_next.py --wrk /absolute/path/to/wrk --build-only --jobs direct-head:base-owned` if needed. Do not compile, run a profiler, or run another load test during throughput measurements. Normal host activity and lack of CPU affinity remain limitations.

All 101 timed screening/comparison samples passed independent status/body/fields, connection reuse and explicit-close preflights. Across 61,698,578 timed requests, wrk reported zero socket/timeout or non-success status errors. Timed wrk runs do not check body bytes. Peer-reset logs consistent with load-generator teardown are retained; no such connection is counted as an extra successful request.

`validate_http.py` applies only the general parser-storage, shared-context, method-constant and response-plan changes to a clean HTTP/1 copy, keeping the original writer policy. All 53 codec, server, proxy and documentation tests passed. The [reviewable general patch](results/http-validation/general-http.patch) and [codec](results/http-validation/base-http1.txt)/[server](results/http-validation/base-http-server.txt) test logs are retained. These tests do not establish conformance of the diagnostic batching or readiness driver.

Benchmark formatting, Clippy with warnings denied, Python syntax checks, and Clippy for the staged final executable/allocation probe passed. The staged executable/probe check on Rust 1.85.1 also passed. All 26 production source/manifest hashes recorded in the initial benchmark remain unchanged.
