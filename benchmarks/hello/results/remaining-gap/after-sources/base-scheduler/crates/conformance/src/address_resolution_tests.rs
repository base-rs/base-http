// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate separately reported `address_resolution` tests. The runner supplies a fresh `AddressResolutionFixture` reference per case.
///
/// Invoke inside a dedicated module. The runner awaits the async closure exactly once, enforces deadlines, drains accepted work on failure, and propagates panics. See the crate guide for runner signatures.
#[macro_export]
macro_rules! address_resolution_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            lazy_and_pre_cancelled => |fixture| {
                $crate::address_resolution::pre_cancelled(fixture).await;
            },
            input_validation => |fixture| {
                $crate::address_resolution::validation(fixture).await;
            },
            ordered_owned_results => |fixture| {
                $crate::address_resolution::results(fixture).await;
            },
            configured_named_service => |fixture| {
                $crate::address_resolution::service_name(fixture).await;
            },
            forwards_all_hints => |fixture| {
                $crate::address_resolution::hints(fixture).await;
            },
            numeric_host_prevents_name_lookup => |fixture| {
                $crate::address_resolution::numeric_only(fixture).await;
            },
            preserves_resolver_errors => |fixture| {
                $crate::address_resolution::errors(fixture).await;
            },
        }
    };
}
