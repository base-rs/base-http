# Owned receive experiment evidence

See the [investigation report](../../owned-receive.md) and [complete tables](tables.md). All 60 samples validate; no throughput samples were removed. `verification.json` separately records expected load-generator teardown diagnostics.

`source.tar.gz` preserves the full remote build source and lockfile. `benchmark.patch` isolates the raw receive experiment. `verified-scheduler-sha256.json` identifies the unchanged scheduler source. `retained-binaries/candidate.gz` is the measured executable; `expected-binaries.sha256` identifies its decompressed hash, the earlier frozen control and wrk. The earlier frozen executable/wrk are retained in the preceding ring-overhead evidence and on Hana.

`diagnostics.tar.gz` retains raw perf data and traces, including the first trace whose parser rejected the worker name. Final trace/profile summaries are unpacked alongside it. `diagnostic-verification.json` reconciles request/response bytes and profile loss checks. Instrumented rates are excluded. Scripts retain the exact remote paths; reconstruct or update those paths when reproducing.
