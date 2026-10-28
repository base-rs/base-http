// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate the in-progress signal-dispatch test. The runner supplies a fresh `SignalsFixture + Sync` reference and enforces its deadline. The fixture is shared with a scoped background dispatch driver.
#[macro_export]
macro_rules! signals_dispatch_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            unsubscribe_retains_in_progress_target_without_joining => |fixture| {
                $crate::signals::in_progress(fixture).await;
            },
        }
    };
}
