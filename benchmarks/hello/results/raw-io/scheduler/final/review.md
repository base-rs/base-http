# Retained readiness notifier review

Independent reviewers assessed the implementation and HTTP duplex/lifecycle requirements. The comparison base is `../before-driver.rs` plus existing scheduler working changes recorded in `../before-scheduler.patch`; HTTP projects have no Git history, so this is a source snapshot review rather than a commit-history review.

- Implementation: satisfied. Monotonic IDs, locked cutoff rechecks and release/acquire publication protect reused slots. All waker callbacks occur outside the notifier lock. Lazy registration rechecks closure/readiness; notification completion survives a competing readiness clear. No required correction was found.
- Lifecycle/duplex: satisfied on the measured macOS host. Production HTTP semantics/framing are unchanged; the five real TCP proxy scenarios and CONNECT/Upgrade tests pass. Raw controls are explicitly diagnostic, not HTTP conformance evidence.
- Validation: canonical workspace coverage 29,117/29,117 measured regions, three existing unreachable exclusions; MSRV libraries/benchmarks and Clippy pass. Existing private Bake tooling requires newer Rust syntax and is excluded from the 1.85.1 check.
- Performance/complexity: satisfied for the scoped private notifier. Independent five-by-five-second screen reduces raw user CPU 11.7%; five-by-eight-second confirmation reduces raw user CPU 9.5% and full HTTP user CPU 5.7%, improving every paired round. No established throughput superiority; Base remains 0.75% below Hyper median throughput in the confirmation.
- Scope: other platforms, TLS, multiworker performance and attribution of the residual CPU difference are not established by these measurements. Profile percentages are wall-clock localization and do not assign an exact CPU share.

Batched routing and a combined atomic tick update were rejected and reverted. The later inline-attribute screen showed no supported benefit and was reverted. Restored source hashes match the measured and reviewed revision exactly.
