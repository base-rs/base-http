# Request target storage

Measured 2026-10-29 on hana (Intel Xeon E3-1240 v6, Arch Linux 7.2.11, Rust 1.99.1). This change replaces the request target's `String` fields with `Scheme`, `Authority` and `Path`. `http` and `https` are borrowed constants, and authorities and paths up to 24 bytes are stored inline. The hello codec allocation probe falls from **2 allocations and 15 bytes to none per request**, and the server no longer allocates a scheme for each request.

## Method

[`instructions.sh`](instructions.sh) runs one server process per sample, pinned to CPU 0, with the ioquatix/wrk fork (4.1.0-22-g556f8db) on CPUs 1–3 and 5–7. After a one-second warm-up, `perf stat` counts the server's user-space instructions, user cycles and task clock during a three-second run, and the script divides each by the requests wrk completed. The Base builds use the native scheduler with the epoll readiness selector. Hyper/Tokio comes from the same benchmark binary as the main build. The builds are main (ae06fe3) and this change, run in five interleaved rounds at each concurrency.

User instructions per request repeat to within about 0.1% across samples, so they are the primary metric. Loopback throughput varies by about ±5%, and this cohort did not record latency.

## Results

Medians of five samples:

| Connections | Build | User instructions/request | User cycles/request | CPU µs/request | Requests/s |
| --- | --- | ---: | ---: | ---: | ---: |
| 64 | main | 6,424 | 4,640 | 5.62 | 175,441 |
| 64 | request targets | 6,186 | 4,304 | 5.48 | 180,513 |
| 64 | Hyper/Tokio | 5,920 | 4,237 | 5.59 | 177,299 |
| 1 | main | 9,002 | 10,603 | 12.53 | 52,487 |
| 1 | request targets | 8,768 | 10,460 | 12.41 | 52,568 |
| 1 | Hyper/Tokio | 8,768 | 10,434 | 12.56 | 52,787 |

At 64 connections, user instructions fall by 238 per request (3.7%) and user cycles by 7.2%. Base still executes 266 more user instructions per request than Hyper, and its user cycles are 1.6% higher. Its total CPU time per request is lower, because kernel work dominates this workload. At one connection, Base matches Hyper's user instructions, and throughput is level across all three builds.

An earlier screen that only removed the server's scheme allocation saved about 128 instructions per request. Removing three allocations saved less than three times that, because inline storage costs copying and branching that a heap pointer does not. Request metadata grows from 224 to 240 bytes on aarch64.

Storing authorities in lowercase, measured afterwards with the same method, costs about 36 user instructions per request at 64 connections (6,185 to 6,221, two samples each). Checking for uppercase letters eight bytes at a time is what keeps it there: a byte-wise check followed by lowercasing cost about 98.

[Samples](results/request-target/samples.jsonl), [summary](results/request-target/summary.json), [allocation probe](results/request-target/allocations.json).
