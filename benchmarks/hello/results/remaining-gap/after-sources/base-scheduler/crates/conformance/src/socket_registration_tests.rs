// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate separately reported `socket_registration` tests. The runner supplies a fresh `SocketRegistrationFixture` reference per case.
///
/// Invoke inside a dedicated module. The runner awaits the async closure exactly once, enforces deadlines, drains accepted work on failure, and propagates panics. See the crate guide for runner signatures.
#[macro_export]
macro_rules! socket_registration_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            imports_established_stream => |fixture| {
                use $crate::SocketRegistrationFixture as _;
                $crate::socket_registration::stream(fixture.scheduler(), fixture.domain()).await;
            },
            imports_listening_socket => |fixture| {
                use $crate::SocketRegistrationFixture as _;
                $crate::socket_registration::listening(fixture.scheduler(), fixture.domain()).await;
            },
        }
    };
}
