// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate separately reported `file_allocate` tests. The runner supplies a fresh `FileAllocateFixture` reference per case.
///
/// Invoke inside a dedicated module. The runner awaits the async closure exactly once, enforces deadlines, drains accepted work on failure, and propagates panics. See the crate guide for runner signatures.
#[macro_export]
macro_rules! file_allocate_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            extend_preserves_data_and_cursor => |fixture| {
                $crate::file_allocate::extend(fixture).await;
            },
            keep_size_reserves_beyond_eof => |fixture| {
                $crate::file_allocate::keep_size(fixture).await;
            },
            lazy_and_pre_cancelled => |fixture| {
                $crate::file_allocate::cancellation(fixture).await;
            },
            validation_and_execution_errors => |fixture| {
                $crate::file_allocate::errors(fixture).await;
            },
        }
    };
}
