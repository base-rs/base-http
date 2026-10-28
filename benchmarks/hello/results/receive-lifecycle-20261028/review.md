# Independent review

Reviewer: `/root/receive_lifecycle_review`, following scheduler
`base-design-review`. Read-only local inspection and independent in-memory
analysis; no measurements, remote commands, builds, tests or edits.

No blocking findings remain. Rejecting the unconditional poll-first candidate is
supported by every concurrent throughput/CPU pair and the absence of a repeatable
equal-rate latency benefit.

## Verified

- Recomputed all paired timing statistics, ranges, clean counter ratios and
  validated response totals independently.
- Checked timing binary hashes and archive differences: only Cargo patch paths
  and scalar Recv SQE `ioprio` change between source snapshots.
- Inspected all clean server affinities, stderr, 100% counter running coverage,
  and all six zero-reported-sample-loss profile headers.
- Reviewed invalid return-probe and build-overlap exclusions, unavailable probe
  miss inventory, failed archival steps and successful task-probe cleanup.
- Corrected one placement claim: Python orchestration was unpinned; the paced
  client's coordinator, server and explicit load placement are distinguished.
  The inaccurate copied unpaced controller metadata is disclosed and retained.

The measurement requirements of scheduler `design/worker-progress.md` are
**satisfied within the rejection experiment's scope**: preserved sources,
equivalent topology/builds, separate unrestricted CPU/throughput and equal-rate
latency, paired ranges, and retained failed/inconclusive cohorts.

Final scheduler behavior is unchanged. CQ publication, idle transitions,
issuance receipts, cancellation and ownership/retirement are structurally
unaffected. New OOB/readiness-clear validation is not applicable because no
runtime candidate is retained.

## Limits

The full load-dependent RTT mechanism, isolated cost of an unsuccessful kernel
receive, older-kernel compatibility of the diagnostic hint and kernel probe
misses remain **not verified**. General cancellation, migration, shutdown and
urgent-data conformance of the rejected candidate were not tested or claimed.
Profiles measure CPU rather than off-CPU delays. These limits do not justify
adopting the candidate and are not presented as passed runtime validation.
