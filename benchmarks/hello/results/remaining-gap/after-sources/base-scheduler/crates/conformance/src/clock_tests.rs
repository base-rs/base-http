// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate separately reported `clock` tests. The runner supplies a fresh `ClockFixture` reference per case.
///
/// Invoke inside a dedicated module. The runner awaits the async closure exactly once, enforces deadlines, drains accepted work on failure, and propagates panics. See the crate guide for runner signatures.
#[macro_export]
macro_rules! clock_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            lazy_registration_and_drop => |fixture| {
                $crate::clock::registration(fixture).await;
            },
            pre_cancelled_waits => |fixture| {
                $crate::clock::pre_cancelled(fixture).await;
            },
            absolute_deadline_and_wakeup => |fixture| {
                $crate::clock::deadline(fixture).await;
            },
            relative_wait_starts_on_poll => |fixture| {
                $crate::clock::relative(fixture).await;
            },
            pending_cancellation_and_wakeup => |fixture| {
                $crate::clock::cancellation(fixture).await;
            },
            expiry_wins_concurrent_cancellation => |fixture| {
                $crate::clock::expiry_wins(fixture).await;
            },
            overflow_registers_no_timer => |fixture| {
                $crate::clock::overflow(fixture).await;
            },
            timeout_results_expiry_and_cleanup => |fixture| {
                $crate::clock::timeout(fixture).await;
            },
        }
    };
}
