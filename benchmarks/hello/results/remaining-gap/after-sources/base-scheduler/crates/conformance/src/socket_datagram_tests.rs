// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate datagram-message tests. The runner supplies `(scheduler, sender, receiver)` as references to fresh bound, unconnected datagram sockets. The scheduler needs `SocketSendMsg`, `SocketRecvMsg`, and `SocketLocalAddress`.
///
/// Invoke inside a dedicated module. Each generated test calls the runner once with an async closure; the runner must create a fresh isolated fixture, await the closure, perform cleanup, enforce a deadline, and propagate panics. Async closures require Rust 1.85 or later.
#[macro_export]
macro_rules! socket_datagram_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            addresses_peek_and_truncation => |scheduler, sender, receiver| {
                $crate::socket_messages::datagram_truncation(scheduler, sender, receiver).await;
            },
            empty_messages_and_boundaries => |scheduler, sender, receiver| {
                $crate::socket_messages::datagram_boundaries(scheduler, sender, receiver).await;
            },
            pending_receive_cancellation => |scheduler, sender, receiver| {
                let _ = sender;
                $crate::socket_messages::pending_cancellation(scheduler, receiver).await;
            },
        }
    };
}
