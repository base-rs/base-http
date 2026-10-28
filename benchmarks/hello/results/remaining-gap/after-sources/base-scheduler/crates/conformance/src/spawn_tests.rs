// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate separately reported `spawn` tests. The runner supplies a fresh `SpawnFixture` reference per case.
///
/// Invoke inside a dedicated module. The runner awaits the async closure exactly once, enforces deadlines, drains accepted work on failure, and propagates panics. See the crate guide for runner signatures.
#[macro_export]
macro_rules! spawn_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            registers_before_first_poll => |fixture| {
                $crate::spawn::registration(fixture).await;
            },
            output_and_handle_transfer => |fixture| {
                $crate::spawn::output(fixture).await;
            },
            abandoned_handle_releases_completed_output => |fixture| {
                $crate::spawn::completed_abandonment(fixture).await;
            },
            preserves_panic_payload => |fixture| {
                $crate::spawn::panic_payload(fixture).await;
            },
            cooperative_cancellation => |fixture| {
                $crate::spawn::cancellation(fixture).await;
            },
            abandonment_destroys_task => |fixture| {
                $crate::spawn::abandonment(fixture).await;
            },
            closed_service_rejects_unpolled_input => |fixture| {
                $crate::spawn::closed(fixture).await;
            },
        }
    };
}
