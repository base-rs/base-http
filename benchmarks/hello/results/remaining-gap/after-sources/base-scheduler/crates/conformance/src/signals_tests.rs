// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate separately reported `signals` tests. The runner supplies a fresh `SignalsFixture` reference per case.
///
/// Invoke inside a dedicated module. The runner awaits the async closure exactly once, enforces deadlines, drains accepted work on failure, and propagates panics. See the crate guide for runner signatures.
#[macro_export]
macro_rules! signals_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            independent_subscriptions => |fixture| {
                $crate::signals::subscriptions(fixture).await;
            },
            multiple_targets_and_signals => |fixture| {
                $crate::signals::multiple_targets(fixture).await;
            },
            setup_errors_release_target => |fixture| {
                $crate::signals::errors(fixture).await;
            },
            reentrant_subscription => |fixture| {
                $crate::signals::reentrant(fixture).await;
            },
        }
    };
}
