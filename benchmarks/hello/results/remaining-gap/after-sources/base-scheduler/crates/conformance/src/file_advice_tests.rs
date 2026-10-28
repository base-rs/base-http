// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate separately reported `file_advice` tests. The runner supplies a fresh `FileAdviceFixture` reference per case.
///
/// Invoke inside a dedicated module. The runner awaits the async closure exactly once, enforces deadlines, drains accepted work on failure, and propagates panics. See the crate guide for runner signatures.
#[macro_export]
macro_rules! file_advice_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            modes_ranges_and_completion => |fixture| {
                $crate::file_advice::modes(fixture).await;
            },
            lazy_admission => |fixture| {
                $crate::file_advice::lazy(fixture).await;
            },
            validation_and_execution_errors => |fixture| {
                $crate::file_advice::errors(fixture).await;
            },
            accepted_work_survives_abandonment => |fixture| {
                $crate::file_advice::abandonment(fixture).await;
            },
        }
    };
}
