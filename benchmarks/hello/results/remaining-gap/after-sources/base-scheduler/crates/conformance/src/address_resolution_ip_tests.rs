// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate numeric and absent-name resolution tests for one supported IP family. The runner supplies a fresh `AddressResolutionFixture` reference; `domain` is a `SocketDomain` expression. Invoke once per supported family in separate modules.
#[macro_export]
macro_rules! address_resolution_ip_tests {
    (run: $run:path, domain: $domain:expr $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            numeric_hosts_and_transport_hints => |fixture| {
                use $crate::AddressResolutionFixture as _;
                $crate::address_resolution::numeric(fixture.scheduler(), $domain).await;
            },
            absent_name_and_passive_hints => |fixture| {
                use $crate::AddressResolutionFixture as _;
                $crate::address_resolution::absent_name(fixture.scheduler(), $domain).await;
            },
        }
    };
}
