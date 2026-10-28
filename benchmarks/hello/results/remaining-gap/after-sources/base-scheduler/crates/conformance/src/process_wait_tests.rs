// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate process-wait cases in a dedicated integration test at crate root. The runner supplies a fresh scheduler reference and retires it after each async case. The two ignored fixtures are invoked only as isolated child processes by the suite.
#[macro_export]
macro_rules! process_wait_tests {
    (run: $run:path $(,)?) => {
        #[test]
        #[ignore = "child fixture, invoked by the process-wait suite"]
        fn process_child_fixture() {
            $crate::process_wait::child();
        }
        #[test]
        fn fixture_cleanup() {
            $crate::process_wait::fixture_cleanup();
        }
        #[test]
        fn exit_status() {
            $run(async |scheduler| {
                $crate::process_wait::exit(scheduler).await;
            });
        }
        #[cfg(windows)]
        #[test]
        fn invalid_or_unsupported_waits() {
            $run(async |scheduler| {
                $crate::process_wait::validation(scheduler).await;
            });
        }
        #[test]
        fn lazy_and_pre_cancelled() {
            $run(async |scheduler| {
                $crate::process_wait::lazy(scheduler).await;
            });
        }
        #[test]
        fn pending_cancellation() {
            $run(async |scheduler| {
                $crate::process_wait::cancellation(scheduler).await;
            });
        }
        #[cfg(unix)]
        #[test]
        #[ignore = "isolated process-group fixture"]
        fn process_group_fixture() {
            $run(async |scheduler| {
                $crate::process_wait::groups(scheduler).await;
            });
        }
        #[cfg(unix)]
        #[test]
        fn process_groups() {
            $crate::process_wait::isolated();
        }
    };
}
