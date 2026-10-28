# Independent Scheduler Interface

## Status and intent

These notes record the design discussion for a future `base-scheduler` crate. This directory is a place to preserve the proposal; no crate, implementation, or migration is being introduced yet. Names and API examples describe the intended boundaries rather than a finalized public API.

The goal is an independent, explicit scheduler interface, similar in purpose to Zig's I/O interface. Protocol libraries should be able to express the operations they need without selecting an executor or event loop. An application selects a concrete implementation at compile time.

## Package boundaries

- `base-scheduler` defines the abstract interface and its behavioral contracts.
- `socketry` provides concrete implementations of that interface.
- `protocol-http` consumes the abstract interface, directly or through a richer protocol stream abstraction. It may use `socketry` as a development dependency for tests.
- Applications depend on a protocol library and their chosen scheduler implementation.
- Other implementation crates can independently provide adapters for existing runtimes or simulated environments.

The normal dependency graph points from both consumers and implementations toward `base-scheduler`. The interface crate must not depend on a concrete scheduler. A protocol library's test-only dependency on `socketry` does not make Socketry a dependency of its published library implementation.

## Interface contents

The interface should contain capability traits such as `File`, `Socket`, `Clock`, and `Spawn`, together with the shared types required to express their contracts. Existing work also includes blocking-operation dispatch and signal notification; their placement should be considered during extraction.

Resource types belong to the implementation through associated types. A file operation should accept the implementation's file resource rather than require `Arc<std::fs::File>`. The same principle applies to sockets, listeners, and task handles. An implementation may use reference counting internally without imposing that representation on every backend.

Cancellation, operation results, buffer ownership, and resource lifetimes need consistent public semantics. Those shared types may contain runtime-independent implementation code. An interface crate can have no concrete scheduler while still providing useful code for cancellation state and future notification.

The interface must not expose Tokio task handles, readiness registrations, or io_uring request records as mandatory public types. Generic consumers should require only the capabilities they use. Higher-level concurrency primitives and task groups can be built above the primitive interface.

## Compile-time selection

Rust traits, generics, and associated types support this arrangement. Protocol code is generic over the relevant capabilities, and the application supplies a concrete scheduler. Trait methods can return futures without requiring boxed futures or trait objects.

The compiler can specialize generic code for the selected implementation, permitting static dispatch and inlining. This does not remove allocations, synchronization, or dispatch costs inside the backend itself.

Compile-time selection does not necessarily require mutually exclusive Cargo features. Multiple implementations can coexist in one program or test binary as different concrete types. Features can control which backend dependencies are compiled where useful.

The exact future bounds, including where `Send` is required, need an explicit decision when extracting the interface. They affect which implementations and execution models can satisfy the contract.

## Protocol abstractions

The scheduler exposes low-level I/O capabilities. Protocol libraries can build richer abstractions above them, such as a stream with buffered reads and line-oriented operations, then compose that stream into an HTTP connection.

An HTTP implementation need not depend directly on every scheduler operation. Its stream implementation may handle the scheduler dependency, while HTTP requests only the additional capabilities it needs, such as timers or task spawning.

## Composing implementations

Task execution, I/O execution, and event waiting are distinct responsibilities:

| Responsibility | Possible implementation |
| --- | --- |
| Execute runnable futures | Socketry's executor, Tokio, or smol |
| Perform I/O operations | Blocking thread pool, readiness plus system calls, or io_uring |
| Wait for events and wake tasks | An independent reactor or the host runtime's reactor |

A scheduler implementation can combine these responsibilities behind one coherent interface. Choosing Tokio for task execution should allow choosing Socketry's io_uring machinery for I/O.

For example, a Tokio adapter could submit operations to a Socketry-owned ring, register the ring descriptor with Tokio through `AsyncFd`, and run a driver task that consumes completion entries and wakes the corresponding operation futures. Readiness signals when to inspect the ring; operation results come from its completion queue. Tokio's [public descriptor registration interface](https://docs.rs/tokio/latest/tokio/io/unix/struct.AsyncFd.html) and [internal io_uring driver](https://docs.rs/tokio/latest/src/tokio/runtime/io/driver/uring.rs.html) provide relevant reference points.

The reusable ring machinery would own operation encoding, buffers, cancellation, and completion handling. It could be driven by a dedicated reactor thread or by a host runtime task. This separation is a proposed implementation direction, not an additional public capability requirement.

A backend that dispatches blocking operations to a thread pool is also possible. Long-lived socket reads and accepts occupy workers, so bounded admission and cancellation behavior need care. Bounded admission does not make an already-running blocking system call interruptible.

Performance must be measured. Batching, contention, task wakeups, and ring ownership can matter as much as the choice of kernel API.

## Behavioral contracts

Independent implementations need compatible observable behavior, beyond matching trait signatures. The interface should specify:

- When an operation begins, including the behavior of an unpolled future.
- Cancellation before submission, after submission, and during completion races.
- Ownership of buffers and resources until backend access has ended, including when the caller drops a future.
- Task-handle ownership, moving handles between tasks, waiting, and dropping handles.
- Clock and deadline semantics.
- Errors, partial results, and required ordering between related operations.
- Resource creation, closure, and scheduler shutdown.

Cancellation remains a cooperative shutdown request. The default interface should not add a separate forced-abort model. Dropping futures or tearing down a runtime is a distinct lifecycle event and must still preserve memory safety while backend operations finish or are retired.

These notes do not redefine the detailed contracts already developed in Socketry. Extraction should review and preserve those decisions deliberately.

## Interface conformance suite

Provide a reusable conformance test suite that an implementation can run against its own scheduler and associated resource types. This is an agreed part of the proposal.

The suite should test observable contracts through the public interface, using backend-supplied setup where necessary. It should be organized by capability so implementations can test the traits they support without pretending to implement unrelated operations.

Coverage should include cancellation and completion races, buffer and resource lifetimes, task ownership, clocks, shutdown, error behavior, and the semantics of individual I/O operations. Tests must allow outcomes explicitly permitted by a contract rather than impose one backend's internal scheduling order.

Use deterministic coordination where possible. Real operating-system integration tests remain necessary for native backends; a simulator is useful for controlled behavior but cannot prove kernel integration correct.

Protocol libraries should also run their own generic behavior tests against multiple backends. Conformance tests validate the scheduler contract, while protocol tests validate how consumers compose it.

The suite could live in a companion support crate or another reusable test package. Decide its packaging later, keeping test infrastructure out of production dependencies. Backend-specific tests remain necessary for internal correctness and platform behavior.

## Starting points for later work

Socketry already has capability traits, associated resource types, multiple backends, and a generic interface probe with a simulator. Review these before introducing a parallel API:

- [Socketry design](https://github.com/base-rs/socketry-rust/blob/b54b8d61bf80b71a1a5b97c3f5b80bae8a5ebabd/context/design.md).
- [Socketry implementation guidance](https://github.com/base-rs/socketry-rust/blob/b54b8d61bf80b71a1a5b97c3f5b80bae8a5ebabd/context/implementation.md).
- [Existing generic interface tests](https://github.com/base-rs/socketry-rust/blob/b54b8d61bf80b71a1a5b97c3f5b80bae8a5ebabd/crates/executor/examples/interface_probe/tests.rs).
- [Existing simulator](https://github.com/base-rs/socketry-rust/blob/b54b8d61bf80b71a1a5b97c3f5b80bae8a5ebabd/crates/executor/examples/interface_probe/simulated.rs).

A future extraction should identify the minimal shared types, settle future bounds and optional capabilities, package the conformance suite, and migrate an implementation plus a representative consumer to validate the boundary. Publication, crate naming, and migration details remain open.
