# Equal offered-rate latency

At a fixed **240,000 requests/s**, Base's borrowed and owned receive modes have approximately the same observed send-to-response p99: **339 µs versus 344 µs**. Their paired median RTT p99 difference is **+1.36%** for owned, rather than the earlier unrestricted wrk cohort's +21.49%. The arrival pattern and client also change, so this is a separate workload, not an isolated proof that rate alone caused the difference.

Including waiting from each request's scheduled arrival, p99 is **6.668 ms borrowed, 4.387 ms owned and 3.484 ms Tokio**. The owned/borrowed ordering varies between rounds; the lower owned median does not establish a reliable latency win.

## Measured comparison

Hana, 2026-10-28: the exact same frozen raw-server executable as the preceding [owned-receive experiment](owned-receive.md), SHA-256 `dc5abb4416113f309c847920d36e75117507ddedd9202756fb978abf2724da6e`. Only the load generator changes. Base borrowed and owned modes retain their identical reused 16 KiB receive storage and borrowed response writes. The Tokio supervisor remains one spawned task. These controls do not parse HTTP messages or exercise production HTTP input policy.

Five interleaved rounds at each of 200k, 220k and 240k requests/s, with two seconds of warmup and eight seconds measured. All 45 samples validate, including **79.2 million measured responses and 99 million including warmup**. Completed rates in the measurement windows stay within **0.03%** of their targets. Every planned request is eventually sent and validated, including requests completing after the offered interval; the largest final drain is 2.90 ms. No client or server reports an error.

All table entries are medians of five per-run percentiles, after merging observations across client workers within each run. They are not pooled percentiles across runs.

| Offered requests/s | Borrowed RTT p99 µs | Owned RTT p99 µs | Tokio RTT p99 µs |
| ---: | ---: | ---: | ---: |
| 200,000 | 270.59 | 270.33 | 189.44 |
| 220,000 | 296.45 | 294.40 | 269.82 |
| 240,000 | 339.45 | 343.81 | 316.16 |

RTT starts at the actual send attempt and ends when the client has read the complete response. It includes socket, server and client response-processing time; it is not a pure network transit measurement. At 240k the paired owned/borrowed RTT p99 changes range from -1.68% to +11.33%, with a +1.36% median. The analogous paired medians at 200k and 220k are -0.09% and -0.69%. The median Base RTT tails are similar at equal offered rates in this cohort; Tokio's median is lower.

| Offered requests/s | Borrowed scheduled p99 ms | Owned scheduled p99 ms | Tokio scheduled p99 ms |
| ---: | ---: | ---: | ---: |
| 200,000 | 0.694 | 0.412 | 0.193 |
| 220,000 | 1.386 | 2.324 | 0.963 |
| 240,000 | 6.668 | 4.387 | 3.484 |

Scheduled latency starts at the planned arrival, before any wait for the preceding response on that connection. Even when average throughput meets the target, temporary delays can create a backlog that persists across later requests. That waiting remains in this distribution and is excluded from actual-send RTT. Percentiles of the component distributions cannot be added or subtracted to reconstruct another percentile.

The scheduled-arrival p99 ranges are broad. At 240k they are **5.390–11.706 ms borrowed, 2.736–9.101 ms owned and 2.449–5.370 ms Tokio**. Owned beats borrowed in three of five pairs, with changes ranging from -69.05% to +68.84%. At 220k owned beats borrowed in two of five pairs. These results establish the observed values and variation, rather than a universal ownership-policy ranking.

## Client and validation

The [Linux client](fixed-rate-client/readme.md) uses pinned busy-polling workers and an absolute schedule distributed evenly across 64 persistent connections. There is one outstanding request per connection. A late response delays that connection's subsequent planned requests without resetting the schedule. This is fixed per-connection arrival pacing, not a shared queue assigning arrivals to any available connection. Pipelining is unsuitable for the raw control, which deliberately has no request framing.

We use a separate client because [wrk2 documents approximately 1 ms pacing accuracy](https://github.com/giltene/wrk2#readme), while the previous p99 differences were below 1 ms. Histogram inputs are nanoseconds with three significant digits; histogram precision does not imply equally precise scheduling. Across all 45 runs, the largest client dispatch p99 is **12.74 µs** and the largest dispatch p99.9 is **21.87 µs**. Dispatch lag measures time after the connection becomes eligible to send; it does not independently measure time spent waiting for the client to observe incoming data. Both RTT and scheduled latency include client effects.

Before measuring, independent client checks verify exact request counts across workers, rejection of corrupted response bytes, and retention of an injected 40 ms server stall in scheduled latency and send delay. The delayed reference produces scheduled p99 30.33 ms and maximum send delay 39.16 ms, showing that queued requests are retained rather than silently omitted. Every benchmark response's full 78 bytes is compared against the expected fixed reply. Measured cohort counts, per-second cohorts, completion counts and histogram counts reconcile. Mean scheduled latency also reconciles with mean RTT plus mean send delay within histogram rounding.

Server threads use CPU 0 and its SMT sibling 4 is unused by the benchmark. Client workers use 2,3,6,7; the controller uses 1. Host, compiler, topology and initial activity are retained with the results. Busy-polling client CPU consumption and request phasing differ from wrk, so the older absolute latencies should not be substituted into this comparison. Background activity is not eliminated, and the ranges retain its effects. No compilation or profiler runs during the timed matrix. This is a single-worker loopback test, not a multicore or independent-client capacity measurement.

The [evidence directory](results/fixed-rate-owned-receive-20261028/) retains every raw client/server output, exact scheduled cohort counts, per-second latency and completion data, [tables](results/fixed-rate-owned-receive-20261028/tables.md), [verification](results/fixed-rate-owned-receive-20261028/verification.json), paired RTT comparisons, client tests, measured client source/lockfile archive, compressed client executable, server/client hashes and reproduction scripts. The server binary is retained by the preceding experiment and referenced by its verified hash. No server, scheduler or HTTP-library implementation changes are involved.
