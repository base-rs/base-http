# Pointer routing churn check

Seven interleaved repetitions per variant and idle count, 2,000 measured rounds after 20 warmup rounds. Same diagnostic fixture as the snapshot experiment: every read publication overlaps a persistent remote thread creating/registering/dropping eight UDP sockets. No builds, checks, profilers or other load tests ran concurrently. Both test binaries and source provenance are preserved.

At 64 idle registrations, original/pointer median user CPU per batch was 12.738/12.5645 microseconds; at 512 it was 12.8755/12.662. Wall/system medians are preserved in summary.json. This diagnostic shows no observed churn regression at these two sizes; it does not establish a general throughput gain. Pointer routing has no map-size-dependent copy on mutation. Failed add/delete quarantine is intentionally exceptional retention until driver stop and is tested separately, not timed here.
