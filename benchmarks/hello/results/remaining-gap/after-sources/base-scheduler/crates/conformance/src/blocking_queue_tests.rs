// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate bounded blocking-queue tests. The runner supplies a reference to a fresh `blocking::BlockingFixture`.
///
/// Invoke inside a dedicated module. Each generated test calls the runner once with an async closure; the runner must create a fresh isolated fixture, await the closure, perform cleanup, enforce a deadline, and propagate panics. Async closures require Rust 1.85 or later.
#[macro_export]
macro_rules! blocking_queue_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            queued_cancellation_and_admission => |fixture| {
                $crate::blocking::queued_cancellation(fixture).await;
            },
            abandoned_handle_does_not_execute => |fixture| {
                $crate::blocking::abandoned_handle(fixture).await;
            },
        }
    };
}
