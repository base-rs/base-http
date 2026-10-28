# Full client comparison rerun — 29 October 2026

This fresh 60-sample run uses the **same frozen executables and measured source archives** as the [preceding comparison](orchestration-20261029.md). No production source, dependency or build changed. The concurrent pooled gap is wider this time: **6.4% for HTTP and 11.0% for HTTPS**, compared with 2.1%/2.3% previously. The previous approximately 2% gap did not reproduce consistently.

Three repetitions use two-second timed phases, 0.5 seconds of warmup, one or 32 persistent connections, an empty GET and a checked 11-byte response. All clients use the same independent two-worker Hyper server. Each client has one worker, with no pipelining; TLS identity, ALPN, version and resumption settings match. No builds, tests or profilers run alongside timing. This is a shared Apple M4 Pro development Mac, without CPU affinity. The cause of the observed run variation has not been isolated.

## Throughput

Median validated responses per second:

| Client | HTTP C1 | HTTP C32 | HTTPS C1 | HTTPS C32 |
|---|---:|---:|---:|---:|
| Previous Base pooled | 34,797 | 96,578 | 30,716 | 91,989 |
| Updated Base pooled | 35,191 | 106,733 | 31,142 | 93,690 |
| Hyper pooled | 38,551 | 114,018 | 31,100 | 105,254 |
| Base connection-only | 35,768 | 130,419 | 32,711 | 107,561 |
| Hyper connection-only | 38,866 | 117,567 | 31,403 | 114,203 |

The updated pooled Base client is 8.7% behind Hyper for HTTP at C1 and approximately equal in HTTPS throughput. Connection-only results also vary: Base leads concurrent HTTP by 10.9%, while Hyper leads concurrent HTTPS by 5.8%. These describe this cohort, not a stable ordering across hosts or runs.

## CPU and variation

Median client CPU microseconds per response at concurrency 32:

| Client | HTTP | HTTPS |
|---|---:|---:|
| Previous Base pooled | 10.325 | 10.837 |
| Updated Base pooled | 9.352 | 10.642 |
| Hyper pooled | 8.751 | 9.478 |
| Base connection-only | 7.626 | 9.257 |
| Hyper connection-only | 8.485 | 8.737 |

Concurrent throughput ranges across the three repetitions:

| Client | HTTP responses/s | HTTPS responses/s |
|---|---:|---:|
| Updated Base pooled | 90,759–107,316 | 88,539–101,408 |
| Hyper pooled | 110,599–118,072 | 101,159–108,782 |

The new median throughput falls relative to the preceding run by 8.7%/12.8% for pooled Base and 4.5%/4.2% for pooled Hyper (HTTP/HTTPS respectively), despite identical executables. That unequal variation materially changes the apparent gap. The earlier five-pair improvement estimate remains a separate cohort; this rerun should not replace it with a more precise claim about the benefit of individual changes. Saturation P99 values were collected at different achieved rates and are not equal-rate latency comparisons.

## Validation and provenance

All **8,505,240 measured responses**, plus **2,123,146 warmup responses**, passed body checks and exact request accounting. Every sample accepted exactly one or 32 connections as specified, with zero server failures. No sample used allocation counters.

The [audit](results/orchestration-full-rerun-20261029/audit.json) verifies the final executable hash and all 76 archived source hashes. The source archive is byte-for-byte identical to the preceding measured archive. Because current project lockfiles have been restored to ordinary registry dependencies, the rerun wrapper retains the original measured snapshot rather than taking a new, potentially misleading snapshot of the working directory.

The frozen updated executable includes the **local, unreleased pool optimization**. Neither the published pool nor registry cache was modified for this rerun. It does not represent an ordinary unpatched pool build.

Normal executable: `/tmp/base-http-orchestration-final-20261029`, SHA-256 `66fe13a6e9305f41fac8bf21cf20653c461ffed8b4b366d24ce8e8c378ba2dd2`.

Baseline executable: `/tmp/base-http-exchange-reuse-final-20261029`, SHA-256 `d6e9658443587be61afa277065f39c962c23e28d5196581eb3d07fd091c42329`.

Raw [summary and ranges](results/orchestration-full-rerun-20261029/summary.json), [metadata](results/orchestration-full-rerun-20261029/metadata.json), [source archive](results/orchestration-full-rerun-20261029/sources.tar.gz), [provenance](results/orchestration-full-rerun-20261029/provenance.json), [exact harness](results/orchestration-full-rerun-20261029/harness.py) and [rerun wrapper](results/orchestration-full-rerun-20261029/rerun.py) are retained alongside every sample and server log.

Reproduce with a new output directory:

```sh
python3 benchmarks/client/results/orchestration-full-rerun-20261029/rerun.py \
  --binary /tmp/base-http-orchestration-final-20261029 \
  --output benchmarks/client/results/orchestration-full-rerun-new \
  --baseline-binary /tmp/base-http-exchange-reuse-final-20261029 \
  --baseline-sources benchmarks/client/results/exchange-reuse-final-20261029/sources.tar.gz \
  --repeats 3 --seconds 2 --warmup 0.5 --concurrency 1,32 \
  --implementations before-open,base-open,base-connection,hyper,hyper-connection
```
