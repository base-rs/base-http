# Equal offered-rate raw server latency — 2026-10-28

All 45 samples validate. Each cell is the median of five per-run p99 values, after merging all client workers within each run. Scheduled latency includes waiting for the connection and client dispatch.

| Offered requests/s | Borrowed p99 ms | Owned p99 ms | Tokio p99 ms |
| ---: | ---: | ---: | ---: |
| 200,000 | 0.694 | 0.412 | 0.193 |
| 220,000 | 1.386 | 2.324 | 0.963 |
| 240,000 | 6.668 | 4.387 | 3.484 |

| Rate | Variant | Scheduled p99 range ms | RTT p99 µs | Dispatch p99 µs | Completed/s median | Delivery fraction range |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 200,000 | borrowed | 0.130–3.226 | 270.59 | 11.94 | 200,000.0 | 99.9999%–100.0041% |
| 200,000 | owned | 0.256–2.173 | 270.33 | 12.03 | 199,999.9 | 99.9992%–100.0281% |
| 200,000 | tokio | 0.110–1.035 | 189.44 | 11.96 | 200,000.0 | 100.0000%–100.0003% |
| 220,000 | borrowed | 0.918–2.030 | 296.45 | 11.87 | 220,000.0 | 99.9999%–100.0000% |
| 220,000 | owned | 0.354–5.960 | 294.40 | 12.16 | 220,000.0 | 99.9981%–100.0014% |
| 220,000 | tokio | 0.563–3.324 | 269.82 | 12.37 | 220,000.0 | 99.9996%–100.0012% |
| 240,000 | borrowed | 5.390–11.706 | 339.45 | 9.98 | 239,989.2 | 99.9732%–100.0001% |
| 240,000 | owned | 2.736–9.101 | 343.81 | 11.52 | 239,997.4 | 99.9982%–100.0027% |
| 240,000 | tokio | 2.449–5.370 | 316.16 | 11.60 | 240,000.0 | 99.9988%–100.0014% |

| Rate | Owned comparison | Paired scheduled p99 change | Pair range | Lower p99 rounds |
| ---: | --- | ---: | ---: | ---: |
| 200,000 | vs borrowed | -3.25% | -92.06%–+315.47% | 3/5 |
| 200,000 | vs tokio | +110.00% | +94.84%–+211.74% | 0/5 |
| 220,000 | vs borrowed | +14.53% | -74.15%–+329.84% | 2/5 |
| 220,000 | vs tokio | +25.35% | -85.20%–+958.18% | 2/5 |
| 240,000 | vs borrowed | -45.88% | -69.05%–+68.84% | 3/5 |
| 240,000 | vs tokio | +25.93% | -25.98%–+243.96% | 1/5 |
