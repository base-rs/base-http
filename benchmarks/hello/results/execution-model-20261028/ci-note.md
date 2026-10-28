# PR CI follow-up

Exact reviewed head: b515ad0928bd8637c05a7abda046abbfdd1c6f5c.

The first Windows job timed out after 15 seconds in the existing
`tokio_adapter_poll` echo smoke test (`benchmarks/echo/tests/smoke.rs:31`).
Seven other smoke tests passed. The retained PR changes no executable runtime
code, and both that smoke test and the Windows/Tokio implementation are unchanged
against the baseline. The raw failed-job log is retained as
ci-windows-failure.log. This is evidence of a CI failure, not a diagnosed root
cause or a claim that the timeout is fixed.

The failed Windows job and dependent aggregate were rerun without code changes.
Linux, macOS, MSRV and release checks passed on the first attempt. Final outcome
will be recorded in ci-final.json and the PR check rollup.

The unchanged-code rerun passed the complete Windows job, including workspace
coverage and all timer configurations, and the aggregate gate. All required PR
checks now pass. The initial timeout did not recur on this rerun; its root cause
remains unverified.
