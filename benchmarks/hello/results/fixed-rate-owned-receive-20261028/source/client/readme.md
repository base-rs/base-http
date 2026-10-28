# Fixed-arrival raw HTTP client

Linux-only diagnostic client for the raw fixed-response controls in this benchmark. It uses four pinned busy-polling OS threads rather than runtime timers, validates every response byte and records HdrHistograms with nanosecond inputs and three significant digits.

```sh
cargo build --release --locked
./target/release/base-http-fixed-rate-client 127.0.0.1:PORT 64 240000 2 8 2,3,6,7
```

Arguments are address, connection count, total offered requests/second, warmup seconds, measured seconds and client CPU list. Start the client controller on a separate CPU, for example `taskset -c 1`. It deliberately consumes every configured worker CPU while running. Its histogram resolution does not imply nanosecond scheduling accuracy; inspect the observed dispatch-lag distribution.

Each connection has an absolute, evenly spaced schedule. Connection `i` sends planned request ordinals `i`, `i + connections`, etc.; request `k` is scheduled at `start + floor(k * 1e9 / rate)` nanoseconds. The schedule never resets to the actual send/completion time. Across connections the planned offered rate is uniform. Connections are distributed round-robin across workers and all workers share one future start instant, after every connection has been established.

Only one request may be outstanding on a connection. When an earlier response is late, subsequent planned requests wait in that connection's implicit backlog; they are sent once it becomes available. That waiting stays in scheduled-arrival latency. There is no HTTP pipelining because the raw control deliberately has no message parser. This defines a fixed rate per connection, not a shared work-conserving pool that reassigns queued arrivals to any free connection.

The measured cohort comprises **all** requests planned during the measured interval, including those sent or completed afterward. The client drains every scheduled request and validates exact cohort counts. It reports a failure if the backlog does not drain within 15 seconds after the offered interval; it never drops late observations to improve percentiles. Completed/sent rates inside the fixed wall-clock window are reported separately, so a target that is eventually completed but not sustained on time remains visible. Per-second latency cohorts and completion counts reveal instability.

Reported latency distributions:

- `scheduled_latency`: complete response time minus planned request time, including connection-backlog and client-dispatch delay.
- `wire_rtt`: complete response time minus actual send attempt, excluding waiting for a connection.
- `send_delay`: actual send attempt minus planned time.
- `dispatch_lag`: actual send attempt minus the later of planned time and the preceding response's observed completion time. This estimates delay after a connection was eligible to send; it includes client scheduling and socket write availability, rather than isolating the server.

Histograms merge all worker observations before computing percentiles. Across repeated server runs the harness reports medians of these per-run percentiles, not a percentile pooled across every run. Use `validate.py` to check exact counts, corrupt response rejection and a deliberately stalled server whose delay must remain visible in both scheduled latency and send delay. `cargo test` also checks the absolute schedule's long-term integer rounding.

The server is measured through the same independent client for all variants. This client's arrival pattern, continuous CPU use and timing costs differ from unpaced wrk, so absolute results are a separate cohort. Its schedule follows the intended-arrival principle described by [wrk2](https://github.com/giltene/wrk2#readme). We use a separate busy-polling client because wrk2 documents approximately 1 ms pacing accuracy, while the earlier raw-server p99 differences were fractions of a millisecond.
