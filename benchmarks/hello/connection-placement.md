# Connection placement and the M6 rerun

Screening evidence, gathered 22 and 23 November 2026 on [hana](https://github.com/base-rs/base-benchmark/blob/main/hosts/hana.md) with base-benchmark 0.4.0 and base-benchmark-load 0.1.0, for [M6](../../design/roadmap.md#m6-version-neutral-connections-and-schedulable-drivers): the hello benchmark against the M5 baseline, base-http main at `9fdc82e`, and the scoped and spawned connection modes of [connection placement](../../design/connection-placement.md) against each other. The [client benchmark's rerun](../client/connection-placement-20261123.md) covers the client. Every campaign is a screening campaign: hana is shared with light services, quiet rather than idle, so none declares an idle host, and the first three campaigns also ran during a ZFS scrub that took about 10% of CPUs 1, 2 and 3. The comparisons, the bisect and the profiles are in [results/m6-20261123](results/m6-20261123/readme.md).

## Decision

- **The scoped mode, with M6's library, costs what M5 did.** M6's interface changes, owned request bodies and the shared `Handler`, cost about 8% per request until base-http#90 removed three costs every request paid for features few requests use; the library is now within noise of M5.
- **`Server::run` is about 11% slower than the old benchmark loop, and that is not an M6 regression.** The M5 baseline's loop served every connection with `Cancellation::never()`, which takes base-scheduler's fast path for socket operations, while M5's own `serve`, which applications used, gave every connection a child cancellation, as `Server::run` does. The benchmark now measures the server applications run. Making cancellable socket operations cheaper is a base-scheduler follow-up after base-http 0.5.0, decided by the user on 2026-11-23.
- **The spawned mode costs nothing measurable on one worker**, within noise of the scoped mode in the final campaign.
- **Two workers on one core's SMT siblings do not help this benchmark**: about 9% slower, at 2.25 times the server CPU per request.

## Method

All campaigns run the `http-closed` load, 64 connections with one request outstanding on each, with two client workers, `--role-cpus client=1,2 --role-cpus server=3`, as under [Placement on hana](readme.md#placement-on-hana), and 10 seeded random rounds unless stated. Placement campaigns give the server CPUs 3 and 7, one core's SMT siblings, so that the two-worker variant has two CPUs; every variant shares them. Trees build with Rust 1.99.1, each with its own lockfile, and the M5 tree has a layout replica, so a change smaller than the replica's is not resolved. The Hyper variants, built in every tree from the same Hyper release, are controls: they move only with the host and build layout.

The `http-closed-placement` scenario's `base-native` gives `Server::run` the scheduler, so it polls every connection within its future; `base-native-spawned` gives it a scope of the scheduler's scoped spawn, so each connection runs as its own task, with one worker, and `base-native-spawned-2` with two. The spawned variants run `Server::run` itself as a task of the scope, so the accept loop runs on a worker, as in `base-native`, and the process has `base-native`'s threads.

## The regression and its bisect

The first comparison of M5 with M6, before base-http#90, put every Base backend 14–22% behind M5 in wall time, and its user CPU per request 34–46% higher; both Hyper controls were within 2%, and M5's replica within 3%. A bisect over every code commit between them, 3 rounds of `base-native` and `hyper-tokio`, located two steps:

| Tree | Change | `base-native` wall time against M5 |
| --- | --- | ---: |
| M5 replica | none | +0.4% |
| `4735ad5` | `ReadableBody` data implements `Chunk` | +3.1% |
| `9a48b13` | Server request bodies become owned values | +4.0% |
| `cb58054` | `Handler` moves into `base-http`, with `ServerContext` | +7.8% |
| `38c4f79` | Selected engines dispatch connections | +7.4% |
| `394464e` | base-scheduler 0.6 | +7.7% |
| `7ae6fac` | `Server` and connection placement | +6.4% |
| `b870b73` | The benchmark serves through `Server::run` | +18.5% |

The Hyper control stayed within 4% throughout. The first step is the library's; the second changes only the benchmark, from its own accept loop to `Server::run`.

### The library step

Neither M5 nor M6 allocates per request, as the allocation probe confirms. User-space profiles of `base-native`, cycles sampled while the same load ran, attributed the step to three costs:

- **A 1,400-byte copy for every request.** The block that awaits a handler while writing any interim responses it hands over was 1,400 bytes, 1,320 of them the interim writer's future, and was moved into `cancellable` for every request. Sampling `memmove`'s size argument found the copy, and `-Zprint-type-sizes` the type. The share of samples in `memmove` rose from 14.4% in M5 to 17.0%.
- **Lending the input to a request that has no body.** Owned request bodies borrow the connection's input through a shared slot, which costs a lock, a slot reference and its release per request, even for a GET whose body has already ended.
- **Locking the interim relay twice per request**, to hand the handler its sender and to finish the exchange.

base-http#90 awaits the handler and each interim write separately cancellable, so the interim writer is pinned in place only when used, and `memmove`'s share returned to 14.5%; it no longer lends input that can never change, with no body, no CONNECT and no upgrade offer; and it hands out senders and finishes exchanges without locking unless an interim response was sent. A campaign of 10 rounds compared M5, `main` before it, and the branch:

| Tree | Wall time | Server CPU per request | User CPU per request | Rounds worse |
| --- | ---: | ---: | ---: | ---: |
| `main` at `7ae6fac` | +7.6% (+1.1 to +16.4) | +8.7% | +18.1% | 10 of 10 |
| base-http#90 | +2.7% (−3.0 to +5.7) | +2.7% | +4.7% | 7 of 10, p = 0.34 |
| M5 replica | +1.8% (−6.8 to +3.2) | +1.2% | +0.9% | 7 of 10 |

The unchanged Hyper control also read +2.7% in base-http#90's build, so its remaining difference is the build's layout or the host, not the library.

### The `Server::run` step

base-scheduler's socket reads and writes take a compact path only when their cancellation `is_never()`: with a real one, every operation that waits also polls a cancellation listener. The benchmark's old loop passed `never`; `Server::run` gives each connection a child of its cancellation, so that cancelling the server reaches every connection, as M5's `serve` did (`crates/server/src/lib.rs:500` at `9fdc82e`). The step from `7ae6fac` to `b870b73` changes nothing else and adds about 11%.

## Final comparison with M5

With base-http#90, against M5's tree, 10 rounds:

| Backend | Wall time | Server CPU per request | User CPU per request | M5 replica, wall time |
| --- | ---: | ---: | ---: | ---: |
| `base-native` | +10.7% (+1.9 to +15.2) | +12.7% | +23.9% | +1.8% |
| `base-iouring` | +10.2% (+3.1 to +15.7) | +11.8% | +24.3% | −0.2% |
| `base-tokio` | +13.2% (+6.7 to +21.1) | +14.8% | +29.9% | +0.0% |
| `base-native-background` | +9.5% (+1.0 to +18.9) | +10.9% | +15.6% | +2.9% |
| `hyper-tokio` | −1.7% (−7.6 to +7.0) | −1.5% | −2.3% | −0.2% |
| `hyper-task-tokio` | −1.4% (−4.5 to +3.0) | −0.8% | −3.4% | +0.5% |

Every Base backend was slower in all 10 rounds, by the `Server::run` step's 10–13%; the library's share is within noise, as above.

## Scoped and spawned

The final campaign, with a layout replica of the candidate:

| Variant against `base-native` | Wall time | Server CPU per request | User CPU per request | Rounds worse |
| --- | ---: | ---: | ---: | ---: |
| `base-native-spawned` | +0.3% (−8.0 to +8.9) | −0.2% | +2.4% | 5 of 10 |
| `base-native-spawned-2` | +9.6% (−5.8 to +16.5) | +125% | +156% | 8 of 10, p = 0.11 |
| Replica of `base-native` | +0.6% (−9.6 to +8.1) | +0.5% | +3.2% | 6 of 10 |

The first campaign, before base-http#90 and without a replica, put the spawned mode 3.3% behind (+1.0 to +11.5) in all 10 rounds; the final campaign's replica range, ±9%, is wider than that difference, so a cost of a few percent is not excluded. A spawned connection's wake goes through the executor's run queue rather than the supervisor's `FuturesUnordered`, and spawning a connection's task allocates it, once per connection.

Two workers more than double the server's CPU per request without raising throughput. The two CPUs are one core's SMT siblings, which share its execution units, so the second worker adds little capacity, and 64 connections with one request outstanding each give it little work to take; the added CPU is probably the second worker looking for work and wakes crossing between the workers, which this campaign did not separate. Two physical cores for the server would need the client on fewer than its two cores, which [the client layout screen](results/client-layout-20261109/readme.md) found limits the load. The spawned mode is for workloads that need more than one worker; this benchmark's hello responses are not one.

## Reproduce

From the repository root, with the M5 tree as a worktree at `../base-http-m5`:

```sh
git worktree add --detach ../base-http-m5 9fdc82e
cargo bake --locked benchmark:prepare --host hana --manifest-path benchmarks/hello/Cargo.toml \
  --tree m5=$PWD/../base-http-m5/benchmarks/hello --tree candidate=$PWD/benchmarks/hello --scenario http-closed
cargo bake --locked benchmark:run http-closed --host hana --manifest-path benchmarks/hello/Cargo.toml \
  --artifacts benchmarks/hello/target/base-benchmark/prepared-TIME/artifacts.json \
  --program load=.cache/base-benchmark-load/bin/base-benchmark-load \
  --role-cpus client=1,2 --role-cpus server=3 --rounds 10
cargo bake --locked benchmark:prepare --host hana --manifest-path benchmarks/hello/Cargo.toml \
  --tree candidate=$PWD/benchmarks/hello --replicas 1 --scenario http-closed-placement
cargo bake --locked benchmark:run http-closed-placement --host hana --manifest-path benchmarks/hello/Cargo.toml \
  --artifacts benchmarks/hello/target/base-benchmark/prepared-TIME/artifacts.json \
  --program load=.cache/base-benchmark-load/bin/base-benchmark-load \
  --role-cpus client=1,2 --role-cpus server=3,7 --rounds 10
```

The bisect and the check of base-http#90 used a temporary scenario of `base-native` and `hyper-tokio` with `http-closed`'s roles, and one tree per commit.
