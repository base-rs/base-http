# Routing snapshot churn

Seven interleaved repetitions per variant and idle count; 2,000 measured rounds after 20 warmup rounds. Real UDP readiness publication wakes a callback that waits for a persistent remote thread to create/register/drop eight UDP sockets. No throughput test or build ran concurrently. Frozen binaries contain the same diagnostic fixture; working sources were restored after compilation. This deliberately forces mutations during every live batch snapshot rather than modeling a connection distribution.

At 512 idle registrations, copy-on-write increases median user CPU per batch from 13.115 to 14.9025 microseconds (13.6%). Wall/system deltas vary: original 161.35/111.19 versus snapshot158.29/114.53 microseconds. At64 idle, user12.7965 versus13.311 microseconds (4.0%). These results establish added CPU during overlapping mutation, not a universal throughput regression. A bounded resolved event batch avoids map-size-dependent copying and is being evaluated instead.
