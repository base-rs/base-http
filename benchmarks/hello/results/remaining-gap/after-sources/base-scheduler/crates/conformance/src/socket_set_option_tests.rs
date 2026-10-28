// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate separately reported `socket_options` tests. The runner supplies a fresh `SocketOptionFixture whose scheduler supports SocketSetOption` reference per case.
///
/// Invoke inside a dedicated module. The runner awaits the async closure exactly once, enforces deadlines, drains accepted work on failure, and propagates panics. See the crate guide for runner signatures.
#[macro_export]
macro_rules! socket_set_option_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            native_values_and_errors => |fixture| {
                $crate::socket_options::set(fixture).await;
            },
            lazy_and_pre_cancelled => |fixture| {
                $crate::socket_options::set_cancellation(fixture).await;
            },
        }
    };
}
