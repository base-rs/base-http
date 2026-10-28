# Echo workload comparison

The [results](results.md) compare real echo traffic and revise the priorities from the synthetic task controls. The [io_uring follow-up](uring-readiness/readme.md) tests the resulting scalar-reader fix.

This runs the existing `base-scheduler` echo workload from scheduler main `f05a630db60686526b6145efb284a9c2f61fe8b4` in a disposable source tree. It tests useful socket work after the [synthetic task controls](../hello/layer-breakdown.md), using one actual task per connection.

`setup.py` adds a `native-poll` mode using Native's default worker-owned epoll, and a corresponding integration smoke test. The existing borrowed echo loop is reused for Base epoll, Base io_uring and the Base/Tokio adapter. The extension explicitly closes borrowed reader/writer handles after EOF, outside the measurement window. Pure Tokio keeps its existing `TcpStream::read` / `write_all` loop. No production scheduler code is changed.

## Workload

- One server task worker; one separately spawned task per persistent TCP connection.
- A fixed client executable for every backend, with four Tokio workers.
- One or 64 connections; 64-byte or 4 KiB payloads; one outstanding round trip per connection.
- Both peers enable `TCP_NODELAY`. Each server connection reuses a 64 KiB buffer and handles partial writes.
- Every echo is checked byte-for-byte by the canonical client. Only round trips entirely within the shared measurement window count.
- One-second warmup and four-second measurement, five interleaved repetitions with seed 20261027. A fresh server/client pair starts for every sample.
- All server threads use CPU 0. Client threads use CPUs 2, 3, 6 and 7. The initial controller uses CPU 1; the client-capacity control uses CPU 5. On Hana, CPU 4 is the server core's unused SMT sibling; the client CPU set spans two physical cores.
- `tokio` uses the canonical multi-thread runtime configured with one worker, rather than the current-thread runtime used in the earlier HTTP comparison. This is a different task/workload arrangement; compare backends within this echo cohort.

| Backend | Executor | Payload operations |
| --- | --- | --- |
| `native-poll` | Native, one worker | Bound borrowed reader/writer on epoll |
| `tokio` | Tokio, one worker | Direct borrowed TCP read/write |
| `tokio-adapter-poll` | Tokio, one worker | Base's bound borrowed reader/writer API |
| `iouring-poll` | Native, one worker | Bound borrowed reader/writer, synchronous nonblocking payload calls and ring readiness |

The io_uring mode does not measure owned-buffer receive/send or multishot reception. Throughput and RTT come from the client's shared timed window. CPU accounting covers each complete process lifetime, including setup, warmup and cleanup; it is only a utilization/headroom diagnostic, not timed-window CPU per echo. Latency is closed-loop RTT, not an overload or open-loop arrival-rate test.

## Run

Use the unmodified scheduler archive retained in [the previous evidence](../hello/results/layer-breakdown-20261027/readme.md). On Linux with the locked dependencies available:

```sh
python3 setup.py --root /tmp/base-http-echo \
  --archive /path/to/scheduler.tar.gz
cargo fmt --manifest-path /tmp/base-http-echo/scheduler/Cargo.toml \
  -p base-scheduler-echo-benchmark
CARGO_TARGET_DIR=/tmp/base-http-echo-target \
  cargo test --release --locked --offline \
  --manifest-path /tmp/base-http-echo/scheduler/Cargo.toml \
  -p base-scheduler-echo-benchmark
CARGO_TARGET_DIR=/tmp/base-http-echo-target \
  cargo build --release --locked --offline \
  --manifest-path /tmp/base-http-echo/scheduler/Cargo.toml \
  -p base-scheduler-echo-benchmark

taskset -c 1 python3 run.py \
  --binary /tmp/base-http-echo-target/release/base-scheduler-echo-benchmark \
  --output /tmp/base-http-echo-results
```

The destination source and results directories must be fresh. The recorded runner keeps raw client JSON, stdout/stderr, sample journals, environment, binary hashes and summaries. `--connections`, `--bytes`, `--backends`, `--repetitions`, `--warmup`, `--seconds`, `--client-workers` and `--client-cpus` restrict or extend the matrix.

To count syscalls separately, add `--library /path/to/counts.so`, using the interposer from the previous layer evidence, and select a fresh output. Those instrumented rates are not throughput evidence. Syscall counts cover the whole server process, including warmup and cleanup; use successful receive counts and read the EOF/setup contributions when interpreting ratios. The interposer cannot observe ring submissions/completions. Do not compile, instrument or profile concurrently with throughput measurements.

`coverage.py` summarizes LLVM region coverage after an instrumented test run; LLVM's tools must match the compiler. Coverage output is separate from the uninstrumented benchmark executable.

For a client-capacity control on Hana, use three client workers on three separate physical cores:

```sh
taskset -c 5 python3 run.py \
  --binary /tmp/base-http-echo-target/release/base-scheduler-echo-benchmark \
  --output /tmp/base-http-echo-headroom --connections 64 \
  --client-workers 3 --client-cpus 1 2 3 --repetitions 3
```

This gives the client another physical core while retaining CPU 0 for the server and leaving its SMT sibling CPU 4 unused. Compare backends within each client configuration.

To hold four client workers fixed while adding the third physical core, use `--client-workers 4 --client-cpus 1 2 3 5 6 7` with the controller pinned to CPU 5. The recorded fixed-worker control restricts backends to `native-poll tokio` and uses three repetitions.

## Compare a frozen baseline

`run.py --baseline /path/to/before` enables the `iouring-before` backend and uses that executable for every client. The ordinary `iouring-poll`, `native-poll` and `tokio` servers use `--binary`. The output records both executable hashes. This keeps the load generator fixed while comparing production scheduler changes:

```sh
taskset -c 1 python3 run.py --binary /path/to/after --baseline /path/to/before \
  --output /tmp/echo-confirmation \
  --backends iouring-before iouring-poll native-poll tokio
```

The original invocation without `--baseline` retains its existing behavior. Source setup and the native echo extension remain the same.
