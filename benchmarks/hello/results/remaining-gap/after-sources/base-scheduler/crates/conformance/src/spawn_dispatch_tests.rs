// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate the active-poll task ownership test. The runner supplies a fresh `SpawnFixture + Sync` reference, supports driving `settle` on a scoped background thread, and enforces a deadline.
#[macro_export]
macro_rules! spawn_dispatch_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            handle_drop_defers_destruction_until_active_poll_returns => |fixture| {
                $crate::spawn::active_poll(fixture).await;
            },
        }
    };
}
