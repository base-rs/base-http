# Full HTTP/1 client comparison rerun — 29 October 2026

The rerun confirms the concurrent pooled-client gap: Base is **8.0% behind Hyper for HTTP and 6.9% for HTTPS at 32 connections** by median throughput. At one connection, pooled Base is 3.1% ahead for HTTP and 1.9% ahead for HTTPS in throughput, although Hyper uses less client CPU. The connection-only comparison depends on concurrency; its concurrent paths are close, while single-connection plaintext Base remains slower.

This repeats the previous full matrix with unchanged frozen executables: three repetitions, two seconds of measurement, 0.5 seconds of warmup, HTTP/HTTPS and concurrency one/32. All five paths use the same independent Hyper server implementation and validate the full 11-byte response. The run uses established persistent connections, no HTTP/1 pipelining, one client worker and two server workers. HTTPS uses the same verified self-signed local identity and TLS/ALPN settings within the run. No builds, tests, profilers or other benchmarks were run concurrently by this agent. The development host remains shared. See the [harness method](readme.md#method).

## Throughput and CPU

Rates are responses/second. CPU is client process CPU µs per completed response. Each median and range uses three samples. `before-open` is the preserved Base implementation before operation-storage reuse; `base-open` is the retained current pooled client. The `*-connection` paths retain a connection and codec state without the high-level pool.

| Transport | Connections | Client | Median responses/s | Range | Median CPU µs/response |
|---|---:|---|---:|---:|---:|
| HTTP | 1 | `before-open` | 32,320 | 32,290–32,408 | 17.17 |
| HTTP | 1 | `base-open` | 32,395 | 32,207–32,469 | 16.90 |
| HTTP | 1 | `hyper` | 31,417 | 31,155–31,705 | 15.76 |
| HTTP | 1 | `base-connection` | 36,215 | 36,106–36,350 | 13.75 |
| HTTP | 1 | `hyper-connection` | 41,271 | 37,793–42,232 | 11.74 |
| HTTP | 32 | `before-open` | 110,768 | 101,483–111,578 | 9.01 |
| HTTP | 32 | `base-open` | 110,287 | 105,796–113,546 | 9.05 |
| HTTP | 32 | `hyper` | 119,823 | 119,026–124,031 | 8.33 |
| HTTP | 32 | `base-connection` | 138,445 | 133,692–144,646 | 7.18 |
| HTTP | 32 | `hyper-connection` | 136,989 | 133,062–139,698 | 7.29 |
| HTTPS | 1 | `before-open` | 30,661 | 30,626–31,171 | 17.86 |
| HTTPS | 1 | `base-open` | 30,808 | 30,648–30,872 | 17.69 |
| HTTPS | 1 | `hyper` | 30,233 | 30,186–30,364 | 17.42 |
| HTTPS | 1 | `base-connection` | 32,959 | 32,900–33,085 | 14.93 |
| HTTPS | 1 | `hyper-connection` | 31,335 | 30,922–31,450 | 15.74 |
| HTTPS | 32 | `before-open` | 100,688 | 97,438–101,522 | 9.92 |
| HTTPS | 32 | `base-open` | 105,610 | 98,885–107,039 | 9.45 |
| HTTPS | 32 | `hyper` | 113,411 | 109,331–114,882 | 8.80 |
| HTTPS | 32 | `base-connection` | 125,547 | 119,010–126,510 | 7.92 |
| HTTPS | 32 | `hyper-connection` | 122,159 | 115,203–125,453 | 8.17 |

At 32 connections, connection-only Base is 1.1% ahead of Hyper for HTTP and 2.8% ahead for HTTPS in median throughput. These small differences have overlapping run ranges and do not establish a portable win. At one connection, connection-only Base is 12.3% behind for HTTP and 5.2% ahead for HTTPS. The results do not support treating every connection-only case as equal.

For the current pooled client versus the previous Base binary, the median per-pair changes at 32 connections are +1.8% for HTTP and +4.9% for HTTPS. At concurrency one they are +0.2% and −0.04%. These are just three pairs per scenario; the [earlier five-pair repeat](exchange-reuse-20261029.md#final-throughput-evidence) and this run show meaningful host variability. The absolute rates are higher than the previous broad run despite identical executables. Preserve each cohort separately rather than attributing that change to code.

## Validation and provenance

All **60 samples** passed full response validation, exact request accounting and exact expected connection counts: **9,025,855 measured responses**, zero server failures. These are ordinary release binaries without allocation counters. All 68 runner-snapshot hashes matched after timing and before the documentation update. Saturation P99 values are not equal-request-rate latency comparisons. This rerun measures macOS on the Apple M4 Pro with Rust 1.87; it does not measure fresh connections or Linux.

Current executable: `/tmp/base-http-exchange-reuse-final-20261029`, SHA-256 `d6e9658443587be61afa277065f39c962c23e28d5196581eb3d07fd091c42329`. Previous executable: `/tmp/base-http-exchange-compact-20261029`, SHA-256 `fc18f5d5acd901dd303bf00350895711fe565db9df39efca4d077647988c0bd6`. Both hashes were verified before execution.

The [original measured-build sources](results/exchange-reuse-full-rerun-20261029/measured-build/sources.tar.gz) and metadata are retained alongside the rerun harness snapshot. The only archived-file difference is benchmark-readme documentation, as recorded in [provenance](results/exchange-reuse-full-rerun-20261029/provenance.json); production source is unchanged. The previous executable has its own baseline sources and hash in the result directory.

Raw [summary](results/exchange-reuse-full-rerun-20261029/summary.json), [metadata](results/exchange-reuse-full-rerun-20261029/metadata.json), [audit](results/exchange-reuse-full-rerun-20261029/audit.json), [paired analysis](results/exchange-reuse-full-rerun-20261029/paired-analysis.json) and all per-sample JSON/server logs are retained.

```sh
python3 benchmarks/client/run.py \
  --binary /tmp/base-http-exchange-reuse-final-20261029 \
  --output benchmarks/client/results/full-rerun-new \
  --baseline-binary /tmp/base-http-exchange-compact-20261029 \
  --baseline-sources benchmarks/client/results/exchange-compact-repeat-20261029/sources.tar.gz \
  --repeats 3 --seconds 2 --warmup 0.5 --concurrency 1,32 \
  --implementations before-open,base-open,base-connection,hyper,hyper-connection
```
