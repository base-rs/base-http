# Equal offered-rate evidence

See the [report](../../fixed-rate.md), [tables](tables.md), [sample validation](verification.json) and [artifact verification](artifact-verification.json). All 45 samples are retained, including wide latency ranges; no late requests are dropped.

`comparison/` contains each client JSON, exact cohort counts, per-second statistics and empty client/server stderr. `screen/` is the separate short harness check. `client-validation/` checks corrupt response rejection and a deliberate stall. `source/client/` and `client-source.tar.gz` preserve the exact measured client source and lockfile; the source archive excludes build products. The current reusable client has a later whitespace-only formatting adjustment. `retained-binaries/client.gz` preserves its measured executable. The frozen server is the preceding owned-receive experiment's `retained-binaries/candidate.gz`, identified by matching hash; it is not duplicated here.

`benchmark.sh` retains the exact interleaved matrix command and affinity. Reproduction requires the archived client/server binaries and the recorded Linux CPU topology, or updated CPU arguments on another host. `rtt-comparison.json` retains paired actual-send RTT comparisons; `comparison/summary.json` retains scheduled-arrival comparisons. Instrumentation and compilation are excluded from the timed matrix.
