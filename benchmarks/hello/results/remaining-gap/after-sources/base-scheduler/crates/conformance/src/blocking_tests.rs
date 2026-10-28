// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate blocking-work lifecycle tests. The runner supplies a reference to a fresh `Blocking` implementation.
///
/// Invoke inside a dedicated module. Each generated test calls the runner once with an async closure; the runner must create a fresh isolated fixture, await the closure, perform cleanup, enforce a deadline, and propagate panics. Async closures require Rust 1.85 or later.
#[macro_export]
macro_rules! blocking_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            lazy_submission => |scheduler| {
                $crate::blocking::lazy(scheduler).await;
            },
            pre_cancelled_submission => |scheduler| {
                $crate::blocking::pre_cancelled(scheduler).await;
            },
            returns_output => |scheduler| {
                $crate::blocking::output(scheduler).await;
            },
            preserves_panic_payload => |scheduler| {
                $crate::blocking::panic_payload(scheduler).await;
            },
        }
    };
}
