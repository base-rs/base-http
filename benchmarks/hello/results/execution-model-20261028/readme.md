# Execution-model study artifacts

See ../../execution-model.md for the mechanism, comparison, decision and limits. Scheduler PR: <https://github.com/base-rs/base-scheduler/pull/66>. Reviewed head: b515ad0928bd8637c05a7abda046abbfdd1c6f5c. Baseline: 5cc179985803e1da812353df73868db668234202.

The immediate-payload prototype is rejected; final scheduler changes retain baseline runtime behavior and add documentation, comments and regression tests. `production-candidate.patch`, `candidate.patch`, and candidate.rs preserve the rejected code/test variant. `scheduler-main.tar.gz` is the tracked baseline; `scheduler-final.tar.gz` is the exact reviewed retained tree.

`fixed-240k/`, `unpaced/`, and analysis.json are the uninstrumented cohorts. The diagnostic sources, stderr prefixes and client JSON are separate builds; strided probes and pseudorandom controls are identified by filename. summary scripts regenerate their analyses. Frozen uninstrumented binaries are compressed under retained-binaries/ and verified against final-binaries.sha256.

Candidate and retained coverage logs are separate. Full workspace validation on Hana is not claimed. The final review allows only comment/measurement-precision amendments after the retained suite, with executable code and assertions unchanged. PR CI records are retained separately when available.

The retained measurement subset, diagnostic patches and replayable summaries are also tracked in scheduler benchmarks/echo/results/worker-reactor/. Both analyses replay byte-for-byte, and applying the diagnostic patches to the baseline rebuilds the saved diagnostic sources exactly. All artifacts remain under the fresh Hana experiment /tmp/base-http-execution-model-20261028; build caches are in home.
