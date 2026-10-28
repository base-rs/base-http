// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate accepted-I/O lifecycle tests. The runner supplies a reference to a fresh `SocketIoFixture`, which guarantees successful progress once released. Invoke separately for each supported operation/input representation.
///
/// Invoke inside a dedicated module. Each generated test calls the runner once with an async closure; the runner must create a fresh isolated fixture, await the closure, perform cleanup, enforce a deadline, and propagate panics. Async closures require Rust 1.85 or later.
#[macro_export]
macro_rules! socket_io_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            completion_wins_cancellation => |fixture| {
                $crate::socket::completion_wins(fixture).await;
            },
            abandoned_operation_retains_input => |fixture| {
                $crate::socket::abandoned_operation(fixture).await;
            },
        }
    };
}
