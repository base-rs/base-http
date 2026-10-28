# Echo server: scheduling and I/O overhead

The report below describes the initial implementation. The subsequent [io_uring reader fix](uring-readiness/readme.md) removes the extra receive and improves concurrent echo throughput by 14–17% in the main confirmation.

The real echo workload does not reproduce the large self-waking-task gap. Native epoll is close to Tokio at one connection and a few percent behind at 64 connections. Separate counters observe **no empty epoll returns for Native** in this workload. The stronger practical lead is receive/wait handling: both io_uring and the Base/Tokio adapter make an additional `WouldBlock` receive per echo.

These results use the existing scheduler echo server, extended with a native epoll mode in a disposable tree. Every connection runs as its own task and every echoed payload is validated. No production scheduler implementation was changed. See the [harness](readme.md) and [evidence](results/20261027-hana/readme.md).

## Initial five-round comparison

One server worker, four client workers on two physical cores, one-second warmup and four-second measurement. Rates are median validated round trips per second:

| Connections | Bytes | Native epoll | Tokio | Base/Tokio adapter | Base io_uring |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 64 | 80,034 | 79,804 | 78,276 | 78,841 |
| 1 | 4096 | 70,846 | 69,911 | 69,274 | 69,516 |
| 64 | 64 | 244,390 | 251,734 | 204,122 | 202,997 |
| 64 | 4096 | 206,967 | 213,863 | 178,277 | 177,242 |

Median within-round native-versus-Tokio throughput changes are +0.65%, +1.34%, -2.56%, and -2.89%, respectively. These paired percentages are not ratios of the table's medians. This is not evidence of a general throughput win or exact parity.

At 64 connections, median p99 RTT is 319.49 µs for native epoll versus 311.04 µs for Tokio with 64-byte messages, and 384.25 versus 367.87 µs with 4 KiB. The adapter trails Tokio by 18.91% and 16.54% in paired throughput; io_uring trails by 19.59% and 16.61%.

## Client placement controls

Giving the client a third physical core changes absolute throughput. A three-worker configuration also changes worker count; a second control holds four workers fixed. Each control has three interleaved rounds at 64 connections, using the same executable and measurement windows.

| Client configuration | Native vs Tokio, 64 bytes | Native vs Tokio, 4 KiB |
| --- | ---: | ---: |
| Four workers, two physical cores, initial cohort | -2.56% | -2.89% |
| Three workers, three physical cores | -2.98% | -2.09% |
| Four workers, three physical cores | -1.86% | Approximately 0.00% |

The four-worker/three-core control gives native/Tokio medians of 229,911/234,015 echoes/s for 64 bytes and 195,663/195,506 for 4 KiB. Neither additional-core configuration raises absolute rates over the initial configuration. Loopback scheduling and batching are sensitive to client placement; these are end-to-end measurements, not proof of isolated server capacity. The changing small native gap should not be described as a fixed cost or as established parity.

The three-worker control includes the other adapters: io_uring remains 12.94–13.05% behind Tokio, and the Base/Tokio adapter 10.43–15.17% behind. Their larger differences persist across client configurations.

## Syscall evidence

A separate instrumented run repeats the initial matrix. Counts cover the whole process, including warmup and cleanup. Normalization uses received payload bytes divided by message size, so it includes all echoed messages rather than dividing whole-process counts by only the timed client count. Instrumented rates are excluded from the throughput comparisons.

At 64 connections with 64-byte messages:

| Backend | Receive calls/echo | `WouldBlock` receives/echo | Epoll calls/echo | Empty epoll returns/echo |
| --- | ---: | ---: | ---: | ---: |
| Native epoll | 1.00072 | 0.00048 | 0.01710 | 0 |
| Tokio | 1.00047 | 0.00023 | 0.02123 | 0.000051 |
| Base/Tokio adapter | 2.00058 | 1.00029 | 0.02060 | 0.000028 |
| Base io_uring | 2.00054 | 1.00025 | Outside interposer coverage | Outside interposer coverage |

The same receive pattern appears at one connection and with 4 KiB payloads: native epoll and ordinary Tokio make approximately one receive per echo; the adapter and io_uring make approximately two. Every backend makes approximately one send per echo. No native empty epoll returns are observed in any of the four cases. With one connection, native epoll makes approximately one productive epoll call per echo, as expected for a task waiting on incoming data.

The adapters' syscall-first borrowed read paths probe an empty socket before waiting. Native's scalar reader and ordinary Tokio use their short-read readiness policy to avoid that ordinary extra probe. This corroborates the [raw HTTP socket control](../hello/layer-breakdown.md), now in a payload-validating echo workload with separately scheduled connection tasks. The counter library cannot observe ring submissions or completions; zero intercepted epoll calls for io_uring does not mean zero kernel readiness work. Instrumentation can also affect scheduling, so the counts are diagnostic observations.

## What to optimize first

The elevenfold self-waking result describes repeated cooperative yielding with almost no useful work. It establishes an inefficient polling cadence for that pattern, but this echo workload spends its driver calls collecting useful events. Changing the task-poll budget is therefore not the demonstrated first fix for echo or HTTP.

The Base/Tokio adapter's gap is useful isolation: a substantial difference exists even with Tokio executing both versions. That points toward adapter receive policy and operation bookkeeping rather than requiring a Native executor explanation. It does not isolate the cost of any one syscall or prove that removing it closes the whole gap.

The next controlled implementation experiment should remove the unnecessary borrowed scalar receive where stream/OOB semantics allow it, separately for io_uring and the Tokio adapter. Preserve flagged/message receive behavior, EOF, urgent-data boundaries, aliasing, cancellation, abandonment and shutdown. Keep the real echo matrix as an acceptance workload and the self-waking controls as scheduler diagnostics. The smaller native epoll gap still warrants profiling useful per-connection work.

## Validation and scope

All 116 uninstrumented measurement samples, 24 short smoke samples and 16 separate syscall samples complete with validated payloads and empty stderr. All six unit and thirteen integration tests pass, including the new native epoll smoke test. Clippy across all benchmark targets passes with warnings denied. The server's LLVM coverage export has 425 merged code regions with none uncovered, including all 26 regions selected for the extension and explicit retirement. Coverage executables are separate from the frozen timing executable.

The source comparison confirms 557 source/manifest files outside the echo benchmark match the pinned archive. The original scheduler checkouts are preserved. The only measured-source changes add a native backend, update usage/tests, and close borrowed capabilities after EOF; no scheduling-policy or reader fast-path optimization is applied.

Hana is a Xeon E3-1240 v6, Linux 7.2.11, Rust 1.99.1, scheduler main `f05a630db60686526b6145efb284a9c2f61fe8b4`, Tokio 1.54.1. All server threads use CPU 0 and its SMT sibling CPU 4 is unused. The canonical echo Tokio runtime uses one worker in its multi-thread scheduler, rather than the current-thread runtime used in the earlier HTTP benchmark. Client configuration and controller affinity are recorded per cohort. Builds, coverage, instrumentation and throughput runs execute in separate phases.

Process CPU percentages include setup, warmup and cleanup; they are not CPU per timed echo. RTT is closed-loop, and these tests do not establish overload latency, multi-worker server scaling, physical-network performance, owned/multishot io_uring performance, or HTTP/Hyper throughput. Separate component timings must not be added to infer request cost.
