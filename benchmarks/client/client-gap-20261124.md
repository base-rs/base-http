# The client gap at 32 connections — 24 November 2026

At 32 connections both clients keep their one core busy, so throughput follows client CPU per response. On hana over plain TCP, Base's direct path took **10.78 µs of client CPU per response, against Hyper's 8.57**, counted by `perf`'s task clock, which includes kernel time. Most of the 2.2 µs gap comes from four parts, which together account for 1.9 µs:

- **The idle-connection probe, 0.67 µs over HTTP and 1.21 µs over HTTPS.** Before each reuse it makes one immediate read, which returns `EAGAIN` and is the only extra system call per request.
- **Formatting the request head through `core::fmt`, about 0.4 µs.** This change removes it.
- **About 0.6 µs more kernel time with identical system calls.** The server sleeps about twice as often per request when serving Base (0.31 against 0.14 voluntary switches), and the client's sends pay for waking it.
- **About 0.3 µs of user cycles.** Base executes 28% fewer user instructions than Hyper but with 1.7 times its L1 data-cache misses, from per-connection state spread across 32 connections.

The change here encodes request heads by appending bytes, as the response writer already does. It saves **949 user instructions per response (20,081 to 19,131, −4.7%) and 8.2% of user cycles** at 32 connections, the same instruction count at one connection and over HTTPS, and 4.1% of client CPU per response by paired median. Its throughput gain, +5.2% paired median at 32 connections over HTTP, is within the host's run-to-run spread and is not claimed. The probe is a deliberate design choice, so it is unchanged; the [proposals](#proposals) describe how base-scheduler could make it free. HTTPS has a further 2.8 µs gap in its TLS path, and the last proposal there covers part of it.

## Setup

All measurements ran on [hana](https://github.com/base-rs/base-benchmark/blob/main/hosts/hana.md), an Intel Xeon E3-1240 v6 (4 cores, 8 threads, SMT siblings N and N+4), Linux 7.2.14 and Rust 1.99.1, with release builds of this benchmark (`debug = 0`). As in the [M6 rerun](connection-placement-20261123.md), the client ran on CPU 1 and the independent two-worker Hyper server on CPUs 2 and 3. Every run held base-benchmark's host lock, and nothing was built on hana while a measurement ran. The host is quiet but shared: Samba and a few `ffmpeg` processes used some CPU throughout.

The baseline, `main`, is `origin/main` at `df3f438` plus `6392482`, which only makes the benchmark record user and system time and context switches per response. The candidate is `b5df65e`, which adds the change. Profiles used separate builds of the same sources with `-C force-frame-pointers=yes -g`, and their throughputs are not used. Diagnostic variants are recorded as patches.

Two kinds of measurement were taken:

- **Interleaved counter runs**, 10 rounds per variant of 3 seconds after 1 second of warmup, with `perf stat` around the client and the server. They give instructions, cycles, cache misses and task clock per response, normalized by the server's request count, which includes warmup. These runs also report context switches, and the server's are read from `/proc` before it stops. `perf` slows the one-connection case, whose processes sleep on every response, so those absolute rates are lower than without it.
- **A formal run with the benchmark's own `run.py`**: 10 repetitions of 3 seconds over HTTP and HTTPS at 1 and 32 connections. It compares main's executable (`before-open`, `before-call`) with the candidate's and with Hyper, rotating the order of implementations.

The [results](results/client-gap-20261124/readme.md) keep every summary, the raw records and the scripts.

## What the gap is made of

At 32 connections over HTTP, per response, median and range over 10 rounds:

| Variant | Responses/s | Instructions:u | Cycles:u | Task clock µs | L1d misses | L1i misses | System µs | Server sleeps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| main | 93,502 (88,917–94,874) | 20,081 (20,070–20,084) | 19,529 (18,986–20,636) | 10.78 (10.57–11.30) | 616 (612–619) | 1866 (1809–1907) | 3.90 (3.79–4.05) | 0.428 (0.365–0.497) |
| candidate | 96,767 (94,145–97,492) | 19,131 (19,130–19,136) | 17,925 (17,716–18,635) | 10.41 (10.29–10.68) | 590 (585–599) | 1726 (1690–1799) | 3.86 (3.80–3.97) | 0.389 (0.372–0.423) |
| candidate, no probe (diagnostic) | 103,404 (100,887–107,870) | 18,983 (18,981–18,988) | 17,805 (17,028–18,278) | 9.74 (9.35–9.96) | 585 (576–589) | 1725 (1696–1783) | 3.58 (3.42–3.74) | 0.306 (0.234–0.353) |
| Hyper | 117,606 (115,463–120,021) | 26,345 (26,292–26,402) | 16,733 (16,384–16,899) | 8.57 (8.39–8.75) | 345 (334–350) | 1542 (1519–1567) | 2.99 (2.82–3.17) | 0.144 (0.135–0.171) |

"System µs" is the client's system time from `getrusage`, and "server sleeps" counts the server's voluntary context switches. The task clock also counts softirq time on the client's CPU, which `getrusage` leaves out. For both clients, task clock times responses per second is about 1.0 CPU, so the client's core is saturated and the throughput ratio matches the ratio of task clocks.

These explanations were ruled out first:

- **TCP behaviour.** Both clients send 2.0 segments and receive 2.0 per response, with no delayed or pure ACKs ([nstat](results/client-gap-20261124/diagnostics/tcp-segments-nstat.txt)). The requests are both 65 bytes and the responses are identical.
- **Client sleeps.** Neither client sleeps: voluntary context switches are below 0.0001 per response.
- **The scheduler's polling.** Under `strace`, Base calls `epoll_pwait` less often per response than Hyper calls `epoll_wait`.
- **System calls, except one.** Hyper makes one `writev` and one `recvfrom` per response. Base makes one `sendto` and **two `recvfrom`, one of which fails with `EAGAIN`**, over HTTP and over HTTPS ([syscalls](results/client-gap-20261124/diagnostics/syscalls-strace.txt), [TLS](results/client-gap-20261124/diagnostics/syscalls-strace-tls.txt)).

### The idle-connection probe

The failing receive is `ResponseReader::probe_viable`. Before reusing an idle connection, it calls `ReadableStream::try_read_into` once. That method's contract is to reach the transport without consulting cached readiness, so it costs one system call even when nothing arrived. The response read that follows uses the selector's cached readiness and makes no extra call.

The diagnostic variant skips that read. At 32 connections it saves 0.67 µs of task clock over HTTP (−6.4%, throughput +6.9%) but only 148 user instructions. Most of the saving is kernel entry and exit, which the host's speculative-execution mitigations make expensive, plus the user-mode IPC the kernel round trip costs. Over HTTPS the probe also drives the TLS receiver, and costs 1.21 µs and 2,053 instructions (throughput +10.0%). At one connection it costs 0.57 µs per response.

The probe is the Async::HTTP viability check that [`design/client-connection.md`](../../design/client-connection.md#one-http-client-pool-contract) requires, and skipping it is a behaviour change. So this change keeps it, and the [first proposal](#proposals) describes a scheduler API that would keep its guarantee in practice without the system call.

### Request head encoding

`request_plan` formatted the request line, the generated fields and `Content-Length` with `write!` into the connection's buffer. That made it 7.7% of the client's cycles inclusive, 3.4% of them in `core::fmt`, while Hyper's head encoding was about 1.6%. The response writer already appends bytes and formats lengths with its own `decimal`; the change makes request heads do the same. The head's bytes are unchanged, and the codec tests check them.

### Kernel time with the same system calls

Without the probe, Base and Hyper make the same system calls, yet Base's client still spends 0.59 µs more in the kernel per response. Its server also sleeps more than twice as often per response, and each sleeping server worker must be woken by the client's `sendto`, a cross-CPU wakeup charged to the client.

The likely cause is the shape of Base's direct path. Each lane reads its response and immediately writes its next request in the same poll, so requests reach the server spaced by the client's per-request work, and a worker that has finished one often sleeps before the next arrives. Hyper's request loops hand requests to separate connection tasks through channels, so one scheduler tick writes a burst of requests.

A diagnostic made each Base lane yield once between requests, so that a tick reads every ready response before any lane writes ([patch](results/client-gap-20261124/patches/diagnostic-switches-on-main.patch), [counters](results/client-gap-20261124/diagnostics/counters-yield.txt)). It cut server sleeps from 0.42 to 0.29 per response but gained no throughput, since the yield costs about 1,000 instructions and 80 L1d misses of its own. The sleeps also fall as the client gets cheaper, from 0.43 for main to 0.31 without the probe. This part shrinks with the others and has no separate fix in the client.

### User cycles and state footprint

Without the probe, Base executes 72% of Hyper's user instructions, 18,983 against 26,345, but takes 6% more user cycles. It has 1.7 times Hyper's L1 data-cache misses (585 against 345) and 12% more instruction-cache misses. At one connection, where everything stays in cache, Base's cycles per response are lower than Hyper's, and so is its latency.

The data misses are spread over the per-connection future state, which is about 8 KiB per connection: the lane's request future (1.7 KiB), the cached opening future (1.8 KiB), request sink (2.3 KiB) and response stream (1.8 KiB), and the codec buffers. Across 32 connections that is about the size of this CPU's 256 KiB L2 cache. The top functions by L1d misses are the benchmark's request future with the client's code inlined (7.3%), the opening future's storage (5.9%) and the response stream's `Checked` (4.3%).

No single function dominates. In the cycle profile, the client's orchestration is about 23%, the HTTP/1 codec 15% and the selector 9%. Cancellation and event listeners are about 7%; much of that is one child `Cancellation` per exchange and one listener registered for the exchange's wait for its response. Response-head parsing is the largest remaining codec cost, 8.3% inclusive, because it passes over the fields several times: parsing them, `framing`, `application_fields` against seven control names, and `has_token`.

### HTTPS

The TLS path has the same extra receive and a larger remainder. Without the probe, Base takes 12.60 µs per response against Hyper's 9.81. It executes fewer instructions (26,328 against 33,034) but 27% more cycles, with more branch, instruction-cache and data-cache misses.

In the TLS profile, event listeners take 5.8% of cycles, and about half of their registrations come from `Cancellation::cancelled` futures. base-endpoint's TLS duplex creates one in `read_into` and in each of its waits, and polls it whenever they are pending. Unlike the selector, the duplex has no fast path for a watched or never signal, so each wait on the exchange's plain cancellation registers a listener. Most of the other registrations are the duplex's own progress listeners.

## The change

`request_plan` now appends the request line and the generated fields directly, and formats `Content-Length` with the response writer's `decimal`.

The counter runs give, per response, median and range over 10 rounds:

| Case | Variant | Responses/s | Instructions:u | Cycles:u | Task clock µs |
| --- | --- | ---: | ---: | ---: | ---: |
| HTTP, 32 connections | main | 93,502 (88,917–94,874) | 20,081 (20,070–20,084) | 19,529 (18,986–20,636) | 10.78 (10.57–11.30) |
| HTTP, 32 connections | candidate | 96,767 (94,145–97,492) | 19,131 (19,130–19,136) | 17,925 (17,716–18,635) | 10.41 (10.29–10.68) |
| HTTP, 1 connection | main | 34,661 (34,226–35,363) | 23,434 (23,434–23,435) | 32,273 (31,351–33,023) | 21.57 (21.23–21.99) |
| HTTP, 1 connection | candidate | 35,176 (34,621–35,882) | 22,485 (22,485–22,486) | 30,291 (29,242–30,979) | 20.88 (20.52–21.28) |
| HTTPS, 32 connections | main | 69,895 (67,794–71,733) | 29,337 (29,289–29,354) | 30,968 (29,988–31,698) | 14.41 (14.07–14.83) |
| HTTPS, 32 connections | candidate | 72,748 (69,970–76,016) | 28,381 (28,351–28,399) | 28,983 (27,885–30,356) | 13.81 (13.26–14.49) |

The instruction saving is the same in every case, 949 to 956 per response, and the instruction ranges don't overlap.

In the formal run, the paired change of each candidate path against main's executable in the same repetition was:

| Transport | Connections | Client | Against | Responses/s | CPU µs/response | Improving pairs |
| --- | ---: | --- | --- | ---: | ---: | ---: |
| HTTP | 1 | `base-open` | `before-open` | +0.9% (-3.2% to +4.6%) | -2.2% (-6.0% to +2.5%) | 5/10 |
| HTTP | 1 | `base-call` | `before-call` | +2.3% (-1.2% to +7.4%) | -4.2% (-7.4% to +1.0%) | 9/10 |
| HTTP | 32 | `base-open` | `before-open` | +5.2% (-5.1% to +14.2%) | -4.1% (-11.2% to +6.2%) | 6/10 |
| HTTP | 32 | `base-call` | `before-call` | +3.7% (-4.4% to +8.3%) | -3.4% (-6.3% to +4.2%) | 8/10 |
| HTTPS | 1 | `base-open` | `before-open` | +2.2% (+0.3% to +4.9%) | -2.9% (-8.0% to +1.0%) | 10/10 |
| HTTPS | 1 | `base-call` | `before-call` | +1.4% (-1.2% to +3.0%) | -2.8% (-6.0% to +1.5%) | 8/10 |
| HTTPS | 32 | `base-open` | `before-open` | +3.5% (-0.1% to +7.5%) | -4.6% (-8.4% to -0.1%) | 9/10 |
| HTTPS | 32 | `base-call` | `before-call` | +3.4% (-1.4% to +5.9%) | -3.4% (-6.5% to +0.3%) | 8/10 |

Its medians, with Hyper:

| Transport | Connections | Client | Median responses/s | Range | Median CPU µs/response | Range |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| HTTP | 1 | `before-open` | 44,129 | 43,176–45,234 | 15.13 | 14.77–15.37 |
| HTTP | 1 | `base-open` | 44,427 | 43,108–45,598 | 14.77 | 14.32–15.14 |
| HTTP | 1 | `hyper` | 41,609 | 40,622–42,218 | 13.55 | 13.07–13.83 |
| HTTP | 32 | `before-open` | 90,204 | 85,580–93,348 | 8.94 | 8.66–9.37 |
| HTTP | 32 | `base-open` | 93,960 | 85,707–98,435 | 8.58 | 8.32–9.45 |
| HTTP | 32 | `hyper` | 117,475 | 114,122–119,246 | 7.25 | 7.14–7.42 |
| HTTPS | 1 | `before-open` | 34,781 | 34,007–35,438 | 19.01 | 18.66–19.99 |
| HTTPS | 1 | `base-open` | 35,609 | 34,983–36,206 | 18.39 | 18.04–19.11 |
| HTTPS | 1 | `hyper` | 34,268 | 33,434–34,742 | 16.51 | 16.07–16.95 |
| HTTPS | 32 | `before-open` | 68,166 | 66,992–69,745 | 12.30 | 11.92–12.56 |
| HTTPS | 32 | `base-open` | 70,995 | 67,220–72,016 | 11.70 | 11.51–12.37 |
| HTTPS | 32 | `hyper` | 102,777 | 97,677–104,162 | 8.41 | 8.30–8.83 |

The `base-call` rows are in the [summary](results/client-gap-20261124/formal/summary.json). Every throughput range overlaps its baseline's, so the change claims instructions and cycles, not throughput.

## Attempts

- **Kept: encoding request heads by appending bytes.** See [the change](#the-change).

- **Diagnostic, not kept: skipping the probe.** This bounds what a free probe would save: 0.67 µs per response over HTTP and 1.21 µs over HTTPS at 32 connections. It changes the documented reuse check, so it waits on the first proposal.

  The first build of this diagnostic read an environment variable on every probe. Its user-time figures were inflated and it was discarded. The measurement on main ([counters](results/client-gap-20261124/diagnostics/counters-noprobe2.txt)) skipped the probe unconditionally; the final one switches it with `DIAG_NO_PROBE`, read once ([patch](results/client-gap-20261124/patches/diagnostic-no-probe-on-candidate.patch)).

- **Diagnostic, rejected: yielding between requests.** It moved writes into bursts, as Hyper's tick does, and cut server sleeps, but did not help throughput. See [Kernel time with the same system calls](#kernel-time-with-the-same-system-calls).

- **Considered, not built: watching the exchange's cancellation.** Over HTTP, an exchange waits on its cancellation once, for its response. `Cancellation::watch` costs a lock, an allocation and a second lock when made, plus a waker clone on the first wait and a lock when dropped, against the single listener registration it would replace. It would also need every I/O call to borrow the one watched value instead of the clones passed today. Over HTTPS it would help only once base-endpoint's duplex polls watched signals, which is the last proposal.

## Proposals

1. **A readiness-aware immediate read in base-scheduler; recommended.** An established, registered selector reader already caches readiness, including a sticky read-closed bit from `EPOLLRDHUP`. A method such as `SocketStreamReader::try_read_ready_into` could return `WouldBlock` without a system call when neither bit is set, and otherwise read as `try_read_into` does. Backends without cached readiness would fall back to `try_read_into`. base-endpoint's `ReadableStream` would forward it, and its TLS duplex would answer from buffered plaintext and its receiver's last outcome.

   The probe would then see input, EOF and errors as of the worker's last selector poll rather than as of the kernel's current state. That is the window Hyper's idle-connection polling has. A connection idle long enough for the server to close it will almost always have been polled since, unless its worker has been busy for that long without polling, so the probe would still find the close. Per this report's measurements, it would save about 0.7 µs per response over HTTP and 1.2 µs over HTTPS.

   This narrows a documented guarantee, so it needs your decision and an update to `design/client-connection.md`. An alternative within base-http alone would probe only connections idle longer than some threshold, as Apache HttpClient's `validateAfterInactivity` does. That needs a clock read per reuse and adds a tunable, so I don't recommend it over the scheduler API.

2. **A watched-signal fast path in base-endpoint's TLS duplex, then a watched exchange cancellation in base-http.** `EncryptedSocketStreamReader::read_into` and the duplex's waits would use `Cancellation::poll_watched` for watched and never signals, as the selector does. The exchange's reads and writes would then borrow one watched signal. This targets most of the 5.8% of TLS cycles spent in event listeners. Measure it in base-endpoint first.

3. **Classify response fields in one pass**, as the request reader's `request_control` already does for requests. Response-head parsing is 8.3% of the client's cycles inclusive, while Hyper's `parse` and `httparse` take 2.4% of its cycles, plus some header-map work. This belongs in base-http and is the next candidate.

4. **Shrink the state each exchange touches.** The L1d misses come from per-connection futures and buffers, and no single type dominates. A useful first step is to measure which cache lines one exchange touches, before changing any future's layout. The scheduled and spawned modes measured alike in [M6](connection-placement-20261123.md), so task placement is not the lever here.

## Reproduce

Use one checkout per revision on the measuring host, each built with `cargo build --locked --release` in `benchmarks/client`. The scripts are in the [results](results/client-gap-20261124/scripts/), with the server always taken from main's executable. `locked.sh` holds the host lock, `final-stat.sh` runs the counter comparisons, and `formal.sh` runs:

```sh
python3 main/benchmarks/client/run.py --binary main/benchmarks/client/target/release/base-http-client-benchmark \
  --output main-sources --toolchain '' --repeats 0
python3 head/benchmarks/client/run.py --binary head/benchmarks/client/target/release/base-http-client-benchmark \
  --output formal --toolchain '' --modes reuse --repeats 10 --seconds 3 --warmup 1 --concurrency 1,32 --transports tcp,tls \
  --implementations before-open,base-open,before-call,base-call,hyper \
  --baseline-binary main/benchmarks/client/target/release/base-http-client-benchmark \
  --baseline-sources main-sources/sources.tar.gz --client-cpus 1 --server-cpus 2,3
```
