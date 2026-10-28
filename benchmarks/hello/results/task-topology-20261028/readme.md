# Matched task-topology evidence

See [the study](../../task-topology.md) for results and limitations.

- `analysis.json`, `summarize.py`: replay all unpaced and fixed-rate comparisons.
- `unpaced/`, `fixed-200k/`, `fixed-240k/`: raw samples, logs and environments.
- `production-source.tar.gz`: exact HTTP, scheduler and wrk sources before probes.
  The manifests contain the original `/tmp/base-http-task-topology-20261028/scheduler`
  patch paths; update them if extracting elsewhere. Scheduler sources are baseline
  `5cc179985803e1da812353df73868db668234202`.
- `production-sources/`: exact built files changed only for separate diagnostics.
- `raw-before.rs`, `main-before.rs`, `raw.patch`, `main.patch`, `raw.rs`, `main.rs`:
  benchmark change and final portable source. Final Linux cfg/Clippy annotations,
  import layout and whitespace differ from the measured sources; Linux executable
  behavior is unchanged. Exact measured files are in `production-sources/`.
- `diagnostic-source.tar.gz`, `diagnostic.patch`, `diagnostic.rs`,
  `make-diagnostics.py`: exact separate probe source and reconstruction procedure.
  `diagnostic-tests.rs` and its logs exercise interleaved suspended writes.
- `diagnostic-*.stderr`, `diagnostic-*.client.json`, `diagnostic-*-validation.json`,
  `diagnostic-validation.json`: complete traces and byte/sample validation.
  The `low` prefix selects 200k; unprefixed diagnostic runs select 240k.
- `diagnostic-analysis.json`, `summarize-diagnostics.py`: per-run trace statistics;
  these are instrumented observations, not production timings.
- `retained-binaries/`: gzip copies of the timing server, independent paced client,
  separate probe server and wrk. SHA manifests retain original binary hashes.
- `build.sh`, `benchmark.sh`, `lower-rate.sh`, `after-timing.sh`,
  `diagnostic-build.sh`, `verification.sh`: original remote commands. `setup.sh`
  records reuse of the preceding source snapshot; the complete resulting source
  archives remove that dependency for reproduction.

To replay statistics locally:

```sh
python3 summarize.py
python3 summarize-diagnostics.py
python3 verify-artifacts.py --scheduler /path/to/base-scheduler-checkout
```

The verification script checks baseline Git source bytes, frozen hashes,
production snapshots, exact probe reconstruction, patch application, byte-for-byte
summary replay, response validation and sample completeness. `SHA256SUMS` covers
all retained artifact files. The independent review is in `review.md`.

For rerunning measurements, extract the production source archive into the
recorded remote path (or update the scheduler manifest patches and script paths),
restore frozen binaries into `binaries/` and `wrk/wrk`, make them executable with
`chmod +x binaries/* wrk/wrk`, and use the recorded
`fixed-run.py` / `http-run.py` commands with fresh output directories. Build and
validation must finish before timing, with the same CPU placement. Probe sources
are rebuilt and run separately after timing; never mix the diagnostic executable
into uninstrumented comparisons. The host-specific throughput is closed-loop;
server saturation, full HTTP and broader scheduler workloads are not established.
