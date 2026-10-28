# Full comparison repeat — Hana, 2026-10-28

Fresh output directories, the same frozen executables and harness, and the same pinned CPU placement as the [original completion-policy comparison](../../ring-overhead/readme.md). Candidate scheduler commit `77fe0f8c9082391d7178f720f3f2d6f08c82eaff`, baseline `587e74620d1dc1468a588f02b7ea1fd37b722db7`. No implementation was rebuilt or changed.

All rates below are medians. Comparisons use the median of within-round percentage changes; they need not equal ratios of the displayed medians. Parentheses give the complete observed paired range, not a confidence interval.

## Concurrent echo: 64 connections

90 samples; 5 rounds, 1-second warmup and 4-second timed window. Every echo payload validates and stderr is empty.

| Server | 64 B echoes/s | 4 KiB echoes/s |
| --- | ---: | ---: |
| Minimal epoll | 268,927 | 227,226 |
| Minimal cooperative io_uring | 258,736 | 219,094 |
| Minimal deferred io_uring | 272,223 | 228,779 |
| Base epoll | 244,643 | 207,479 |
| Base borrowed, cooperative | 237,689 | 202,100 |
| Base borrowed, deferred | 246,721 | 206,688 |
| Tokio | 248,524 | 207,155 |
| Base owned, cooperative | 260,214 | 221,241 |
| Base owned, deferred | 268,588 | 223,306 |

| Throughput comparison | 64 B paired change (range) | 4 KiB paired change (range) |
| --- | ---: | ---: |
| Base borrowed, deferred vs Base borrowed, cooperative | +4.84% (+3.17% to +5.47%) | +2.45% (-0.12% to +4.06%) |
| Base borrowed, deferred vs Base epoll | +1.68% (+0.66% to +2.20%) | +0.86% (-2.25% to +1.55%) |
| Base borrowed, deferred vs Tokio | +0.22% (-0.73% to +0.96%) | -0.83% (-2.91% to +4.19%) |
| Base owned, deferred vs Base owned, cooperative | +1.07% (+0.89% to +4.85%) | +1.14% (-0.86% to +2.85%) |
| Base owned, deferred vs Tokio | +7.44% (+7.37% to +9.67%) | +7.56% (+6.74% to +10.13%) |
| Base epoll vs Tokio | -1.80% (-2.37% to +0.30%) | -0.61% (-1.68% to +2.60%) |
| Minimal cooperative io_uring vs Minimal epoll | -3.79% (-4.52% to -3.34%) | -3.33% (-4.04% to -1.80%) |
| Minimal deferred io_uring vs Minimal epoll | +1.23% (+1.02% to +1.98%) | +0.68% (-0.46% to +3.02%) |

| Deferred borrowed p99 RTT | 64 B paired change (range) | 4 KiB paired change (range) |
| --- | ---: | ---: |
| vs Base borrowed, cooperative | -3.70% (-14.12% to -0.64%) | -2.41% (-15.69% to +5.85%) |
| vs Tokio | -1.23% (-3.59% to +1.71%) | -4.38% (-12.33% to +5.56%) |

## Single-connection echo

36 samples; 3 rounds, 1-second warmup and 4-second timed window. Every echo payload validates and stderr is empty.

| Server | 64 B echoes/s | 4 KiB echoes/s |
| --- | ---: | ---: |
| Base borrowed, cooperative | 79,119 | 68,601 |
| Base borrowed, deferred | 82,578 | 71,326 |
| Base epoll | 79,062 | 70,394 |
| Tokio | 78,368 | 69,437 |
| Base owned, cooperative | 82,170 | 70,786 |
| Base owned, deferred | 84,606 | 72,677 |

| Throughput comparison | 64 B paired change (range) | 4 KiB paired change (range) |
| --- | ---: | ---: |
| Base borrowed, deferred vs Base borrowed, cooperative | +4.22% (+2.01% to +4.37%) | +4.46% (+2.01% to +4.46%) |
| Base borrowed, deferred vs Base epoll | +2.21% (+1.19% to +4.46%) | +0.85% (+0.33% to +1.80%) |
| Base borrowed, deferred vs Tokio | +5.37% (+0.14% to +5.75%) | +1.44% (+1.36% to +3.21%) |
| Base owned, deferred vs Base owned, cooperative | +2.96% (-1.24% to +4.74%) | +2.90% (+1.32% to +2.93%) |
| Base owned, deferred vs Tokio | +5.65% (+5.17% to +9.26%) | +3.54% (+3.47% to +4.75%) |
| Base epoll vs Tokio | +1.23% (-2.02% to +4.13%) | +1.11% (+0.50% to +1.38%) |

| Deferred borrowed p99 RTT | 64 B paired change (range) | 4 KiB paired change (range) |
| --- | ---: | ---: |
| vs Base borrowed, cooperative | +0.47% (-3.23% to +0.73%) | -1.65% (-5.28% to +8.21%) |
| vs Tokio | -1.23% (-7.15% to +2.32%) | +6.29% (-2.01% to +7.53%) |

## Full HTTP

20 samples; five rounds, 64 persistent connections, 1-second warmup and 4-second timed window. All response checks pass; wrk reports no socket/status errors. Expected reset logs at forced teardown are retained and counted separately.

| Server | Requests/s | Timed CPU µs/request | p99 latency ms |
| --- | ---: | ---: | ---: |
| Base io_uring, cooperative | 166,421 | 4.558 | 0.595 |
| Base io_uring, deferred | 178,366 | 4.198 | 0.507 |
| Base epoll | 173,353 | 4.362 | 0.575 |
| Hyper/Tokio, runtime root | 176,407 | 4.245 | 0.490 |

| Deferred Base vs | Throughput change (range) | CPU/request change (range) | p99 change (range) |
| --- | ---: | ---: | ---: |
| Base io_uring, cooperative | +5.05% (+1.86% to +8.42%) | -5.26% (-8.95% to -3.92%) | -11.60% (-22.00% to -4.95%) |
| Base epoll | +1.66% (-1.11% to +4.12%) | -2.17% (-4.26% to +0.16%) | -8.52% (-13.33% to -2.37%) |
| Hyper/Tokio, runtime root | +0.48% (-6.22% to +1.11%) | -0.15% (-1.09% to +6.30%) | -0.82% (-12.44% to +30.20%) |

## Raw fixed response

20 samples; five rounds, 64 persistent connections, 1-second warmup and 4-second timed window. All response checks pass; wrk reports no socket/status errors. Expected reset logs at forced teardown are retained and counted separately.

| Server | Requests/s | Timed CPU µs/request | p99 latency ms |
| --- | ---: | ---: | ---: |
| Base io_uring, cooperative | 224,149 | 3.210 | 0.389 |
| Base io_uring, deferred | 233,968 | 3.020 | 0.372 |
| Base epoll | 232,626 | 3.051 | 0.367 |
| Tokio, spawned connection tasks | 237,920 | 2.974 | 0.380 |

| Deferred Base vs | Throughput change (range) | CPU/request change (range) | p99 change (range) |
| --- | ---: | ---: | ---: |
| Base io_uring, cooperative | +5.48% (+2.89% to +7.60%) | -6.49% (-9.56% to -4.13%) | -6.53% (-8.97% to +3.08%) |
| Base epoll | +0.92% (+0.12% to +3.88%) | -1.25% (-4.73% to -0.12%) | -1.43% (-14.94% to +9.26%) |
| Tokio, spawned connection tasks | -0.94% (-4.10% to +0.01%) | +0.95% (-0.02% to +5.00%) | -5.26% (-9.45% to +10.16%) |

## Repetition against the previous cohort

These are separate measurements of identical executables. Do not pool their absolute rates or substitute one workload for another.

| Paired throughput comparison | Previous | Repeat |
| --- | ---: | ---: |
| 64 B iouring-deferred_vs_iouring-poll | +5.78% | +4.84% |
| 64 B iouring-deferred_vs_tokio | -0.25% | +0.22% |
| 64 B iouring-owned-deferred_vs_tokio | +8.09% | +7.44% |
| 4096 B iouring-deferred_vs_iouring-poll | +2.86% | +2.45% |
| 4096 B iouring-deferred_vs_tokio | -0.13% | -0.83% |
| 4096 B iouring-owned-deferred_vs_tokio | +6.94% | +7.56% |
| http after_vs_before | +8.01% | +5.05% |
| http after_vs_hyper | +1.70% | +0.48% |
| http-raw after_vs_before | +3.98% | +5.48% |
| http-raw after_vs_tokio | -2.37% | -0.94% |

## Method and limits

Hana was idle at preflight (about 99% aggregate idle). Server CPU 0, SMT sibling CPU 4 unused, load generator CPUs 2,3,6,7, controller CPU 1. Linux 7.2.11 on Xeon E3-1240 v6, the same Rust-built binaries and Tokio 1.54.1 as before. No builds, profiling or heavy analysis ran concurrently with throughput.

Owned-buffer and borrowed-buffer servers use different execution models. Minimal event loops contain no scheduler tasks. Full Hyper uses the existing runtime-root layout while the raw Tokio control uses spawned connection tasks. Keep those distinctions when interpreting comparisons.

Echo CPU accounting covers the whole process lifetime, including setup/warmup/cleanup, so it is not timed CPU/request. HTTP CPU/request uses the timed window. The prior off-host Wi-Fi check is not repeated: it did not saturate Hana or establish maximum scaling. This repeat measures loopback echo and static HTTP, not general production capacity.

## Retained evidence

`repeat.sh` contains exact commands. `provenance.json`, `expected-binaries.sha256`, initial/final host/socket state, raw client/wrk output, environment JSON and logs are retained. The binary checks run before and after the matrix. `validated-summary.json` checks complete matrices, raw echo JSON, successful response probes and stderr/error expectations. The frozen executables and source archives remain in the original evidence directory, referenced by hash. `sha256.json` hashes the repeat files.

Revalidate from the repository root with `python3 benchmarks/echo/ring-overhead/summarize.py benchmarks/echo/results/ring-overhead-repeat-20261028 --http-cohorts http http-raw`. Run this `report.py` after validation to regenerate the tables.
