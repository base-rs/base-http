// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate prepared stream-message tests. The runner supplies `(scheduler, sender, receiver)` as references to a fresh connected pair. The scheduler needs the public `SocketTestScheduler` fixture profile.
///
/// Invoke inside a dedicated module. Each generated test calls the runner once with an async closure; the runner must create a fresh isolated fixture, await the closure, perform cleanup, enforce a deadline, and propagate panics. Async closures require Rust 1.85 or later.
#[macro_export]
macro_rules! socket_message_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            pre_cancelled => |scheduler, sender, receiver| {
                $crate::socket_messages::pre_cancelled(scheduler, sender, receiver).await;
            },
            prepared_transfers => |scheduler, sender, receiver| {
                $crate::socket_messages::transfer(scheduler, sender, receiver).await;
            },
            pending_receive_cancellation => |scheduler, sender, receiver| {
                let _ = sender;
                $crate::socket_messages::pending_cancellation(scheduler, receiver).await;
            },
        }
    };
}
