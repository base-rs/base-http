# Independent review

Reviewer: `/root/topology_review`, following scheduler `base-design-review`.
The reviewer performed read-only inspection and independent in-memory replay;
they did not edit code or run builds, tests, remote commands or measurements.

The sole documentation finding concerned admission: the unpaced warmup and
measurement use different wrk processes, so admission is included in measurement.
The study was corrected to distinguish that from paced setup. The reviewer
confirmed the exact corrected wording and reported no substantive findings
remaining. The following scope limits remain.

## Verified independently

- Shared 16 KiB initialized heap buffers and one-read/one-response work; actual
  spawned-supervisor and per-connection task topologies; retained task handles.
- All 4,012 tracked scheduler baseline files exactly match the production archive.
  Exact built snapshots and all four retained binary hashes match. The final
  benchmark source cleanup differs only by disclosed Linux annotations, import
  layout and whitespace.
- Every paired comparison in `analysis.json`, all paced response totals and empty
  paced stderr. The study distinguishes medians from paired changes, actual RTT
  from scheduled latency, and wrk errors from peer-reset shutdown logs.
- In-memory source-generator replay exactly matches the diagnostic archive.
  Every stage statistic, all 12 sample counts and all row-sum invariants match.
  The per-connection pending map fixes attribution across interleaved writes.
- Probe timestamp limitations, perturbation and the absence of precise production
  latency attribution are accurately stated.

The applicable performance measurement requirements in scheduler
`design/worker-progress.md` and the research-method constraints in
`context/execution.md` are satisfied within this experiment's scope.

## Not verified or outside scope

- Full HTTP, allocations, parser copies, retained memory, streaming, full duplex,
  backpressure, slow readers and multiple workers.
- Server-only saturation: observed server process CPU is about 70–71% of one core.
- The precise mechanism behind the load-dependent Tokio gap; probes begin after
  successful receive publication and alter execution.
- General runtime cancellation, retirement, shutdown or urgent-data conformance;
  executable scheduler behavior is unchanged.
- Build/test success was inspected through retained logs rather than independently
  rerun by the reviewer.

The evidence supports retaining the benchmark modes and further measurement. It
does not establish a universal topology or scheduler-policy improvement.
