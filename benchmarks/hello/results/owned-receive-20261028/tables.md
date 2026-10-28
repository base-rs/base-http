# Owned receive raw HTTP control — 2026-10-28

All 60 uninstrumented samples validate. Rates and latency below are medians of five rounds. Paired changes are medians of within-round ratios, with their complete ranges.

| Connections | Variant | Requests/s | CPU µs/request | p99 ms | RSS KiB after |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | borrowed | 69,260 | 6.199 | 0.025 | 3,360 |
| 1 | owned | 69,142 | 6.723 | 0.025 | 3,488 |
| 1 | frozen | 68,726 | 6.252 | 0.026 | 3,416 |
| 1 | tokio | 68,031 | 5.935 | 0.026 | 3,016 |
| 16 | borrowed | 233,490 | 3.051 | 0.106 | 3,680 |
| 16 | owned | 237,638 | 2.915 | 0.135 | 3,756 |
| 16 | frozen | 234,434 | 3.049 | 0.109 | 3,664 |
| 16 | tokio | 235,115 | 3.009 | 0.104 | 3,252 |
| 64 | borrowed | 235,303 | 2.999 | 0.380 | 4,540 |
| 64 | owned | 255,578 | 2.751 | 0.458 | 4,644 |
| 64 | frozen | 236,361 | 2.982 | 0.369 | 4,544 |
| 64 | tokio | 242,435 | 2.908 | 0.357 | 4,048 |

| Connections | Owned comparison | Throughput change [range] | CPU/request change [range] | p99 change [range] |
| ---: | --- | ---: | ---: | ---: |
| 1 | vs borrowed | +0.55% [-0.27, +1.80] | +9.23% [+4.71, +10.16] | +4.00% [-7.69, +19.05] |
| 1 | vs tokio | +1.63% [+1.04, +2.91] | +13.28% [+10.05, +17.61] | +0.00% [-3.85, +4.35] |
| 16 | vs borrowed | +1.28% [-0.78, +4.89] | -4.26% [-6.95, -1.65] | +24.53% [+21.90, +29.36] |
| 16 | vs tokio | +1.07% [-2.04, +3.00] | -2.76% [-3.93, +0.00] | +29.81% [+26.92, +34.62] |
| 64 | vs borrowed | +8.62% [+6.04, +11.55] | -8.24% [-11.89, -7.93] | +21.49% [+8.17, +28.01] |
| 64 | vs tokio | +5.42% [+2.10, +6.81] | -5.37% [-7.35, -3.74] | +29.33% [+21.81, +33.33] |
