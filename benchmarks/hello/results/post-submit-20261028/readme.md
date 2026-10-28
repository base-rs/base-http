# Post-submit completion evidence

See [the report](../../post-submit-completions.md) for conclusions and limitations.

- `fixed-240k/`: 20 fixed-rate samples, 38.4M measured / 48M total validated responses; per-run client histograms and environment metadata.
- `unpaced/`: 40 unrestricted throughput/CPU samples at one and 64 connections, all with zero errors.
- `analysis.json`, `summarize.py`: derived medians and within-round comparisons.
- `diagnostic-*.rs`, `diagnostic-*.json`, `diagnostic-*.stderr`, `diagnostic-summary.json`: separate instrumented builds and partial cumulative counters. Their timing is excluded from performance comparisons.
- `scheduler.patch`, `source-revision.txt`, `scheduler-candidate.tar.gz`: final scheduler change against main. `scheduler-with-regression.tar.gz` / `scheduler.tar.gz` preserve the baseline source with the initial failing regression.
- `experiment-source.tar.gz`: remote final scheduler and HTTP source, exact lockfiles and runners; excludes compilation output. Its changed scheduler files match the local checkout.
- `retained-binaries/candidate.gz`, `binaries.sha256`, `final-binaries.sha256`: frozen uninstrumented candidate and artifact identities. The unchanged baseline/client are retained by the earlier owned-receive and fixed-rate experiments.
- `regression-before.log`, `regression-after.log`, `regression-observed.json`: one-test expected failure and pass. See `regression-note.md` for the excluded initial zero-test filter mistake.
- `coverage-final.log`: shared Bake coverage passes 7,552/7,552 measured regions; three invariant unreachable regions excluded. `coverage.sh` records the Arch compiler-source-path reporting workaround. Earlier unsuccessful fixture/coverage attempts are retained in separate logs.
- `clippy.log`: final all-targets/all-features Clippy with warnings denied passes; `clippy-initial.log` records the resolved fixture warning.
- `artifact-verification.json`, `archive-sha256.txt`, `binary-verification.log`: source/binary/archive checks.

The candidate changes production completion publication only. The later coverage fixture edits are test-only and do not require rebuilding or repeating the timed executable. The raw benchmark bypasses HTTP framing and does not establish full HTTP or multicore performance.
