# M6 rerun results, 22 and 23 November 2026

The data behind [connection placement and the M6 rerun](../../connection-placement.md). Each campaign's `metadata.json` is base-benchmark's plan and host record: the trees and their Git state, artifact hashes, CPU placement, rounds and seed. Each CSV is written from the campaign's `summary.json` by [pairs.py](pairs.py), one row per compared pair and metric: the median change over rounds and its range, as fractions, the rounds that improved and regressed, and the sign test's p. The samples and full summaries stay with the campaigns, on hana under `~/.cache/base-benchmark` and locally under `benchmarks/hello/target/base-benchmark`.

| Files | Campaign | Trees | Rounds |
| --- | --- | --- | ---: |
| `1-before-fix-http-closed` | `http-closed`, during the ZFS scrub | M5 (`9fdc82e`) with a replica, and the candidate at `b870b73`, before base-http#90 | 10 |
| `2-before-fix-placement` | `http-closed-placement`, during the scrub, no replica | The candidate at `b870b73` | 10 |
| `3-bisect` | `base-native` and `hyper-tokio` under `http-closed`'s roles, during the scrub | M5 with a replica, and `4735ad5`, `9a48b13`, `cb58054`, `38c4f79`, `394464e`, `7ae6fac` and `b870b73` | 3 |
| `4-fix-check` | As `3-bisect` | M5 with a replica, `main` at `7ae6fac`, and base-http#90's branch | 10 |
| `5-final-http-closed` | `http-closed` | M5 with a replica, and the candidate on base-http#90 (`231671f`) | 10 |
| `6-final-placement` | `http-closed-placement`, with a replica | The candidate on base-http#90 | 10 |

`pairs.py SUMMARY.json > OUT.csv` regenerates a CSV from a campaign's summary.
