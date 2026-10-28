// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate scalar stream/socket tests. The runner supplies `(scheduler, sender, receiver)` as references to a fresh connected pair. The scheduler needs `SocketTestScheduler` and `SocketShutdown`.
///
/// Invoke inside a dedicated module. Each generated test calls the runner once with an async closure; the runner must create a fresh isolated fixture, await the closure, perform cleanup, enforce a deadline, and propagate panics. Async closures require Rust 1.85 or later.
#[macro_export]
macro_rules! socket_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            pre_cancelled => |scheduler, sender, receiver| {
                $crate::socket::pre_cancelled(scheduler, sender, receiver).await;
            },
            transfer => |scheduler, sender, receiver| {
                $crate::socket::transfer(scheduler, sender, receiver).await;
                $crate::socket::shutdown(scheduler, sender, receiver).await;
            },
            shutdown_and_eof => |scheduler, sender, receiver| {
                $crate::socket::shutdown(scheduler, sender, receiver).await;
            },
            pending_read_cancellation => |scheduler, sender, receiver| {
                let _ = sender;
                $crate::socket::pending_read_cancellation(scheduler, receiver).await;
            },
            pending_borrowed_read_cancellation => |scheduler, sender, receiver| {
                let _ = sender;
                $crate::socket::pending_borrowed_cancellation(scheduler, receiver).await;
            },
        }
    };
}
