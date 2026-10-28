// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{Blocking, BlockingError, Cancellation, TaskError};
use base_scheduler_conformance as conformance;
use futures_lite::future::block_on;
use std::{
    future::{Ready, ready},
    panic::{AssertUnwindSafe, catch_unwind},
    process::{Command, Output},
};

// This focused scheduler covers the blocking lifecycle suite. Only child-process tests enable the deliberate contract violation.
struct Scheduler {
    ignore_cancellation: bool,
}
impl Blocking for Scheduler {
    type Handle<T>
        = Ready<Result<T, TaskError>>
    where
        T: Send + 'static;

    async fn spawn_blocking<F, T>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<Self::Handle<T>, BlockingError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        if cancellation.is_cancelled() && !self.ignore_cancellation {
            return Err(BlockingError::Cancelled);
        }
        Ok(ready(
            catch_unwind(AssertUnwindSafe(operation)).map_err(TaskError::Panicked),
        ))
    }
}

fn run_case(test: impl AsyncFnOnce(&Scheduler)) {
    let scheduler = Scheduler {
        ignore_cancellation: std::env::var_os("CONFORMANCE_REPORTING_FAULT").is_some(),
    };
    block_on(test(&scheduler));
}

mod generated {
    // A qualified runner path also checks expansion without any caller-side support imports.
    crate::conformance::blocking_tests!(run: crate::run_case);
}

fn child(arguments: &[&str], broken: bool) -> Output {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(arguments)
        .env_remove("CONFORMANCE_REPORTING_FAULT");
    if broken {
        command.env("CONFORMANCE_REPORTING_FAULT", "1");
    }
    command.output().unwrap()
}

#[test]
fn generated_cases_are_listed_filterable_and_report_failures_independently() {
    let listing = child(&["generated::", "--list"], false);
    assert!(listing.status.success());
    let listing = String::from_utf8(listing.stdout).unwrap();
    assert!(listing.contains("generated::pre_cancelled_submission: test"));
    assert!(listing.contains("generated::returns_output: test"));

    // The deliberate cancellation failure must not prevent sibling cases from running.
    let failing = child(&["generated::", "--test-threads=1"], true);
    assert!(!failing.status.success());
    let report = String::from_utf8(failing.stdout).unwrap();
    assert!(
        report.contains("generated::pre_cancelled_submission ... FAILED"),
        "{report}"
    );
    assert!(
        report.contains("generated::returns_output ... ok"),
        "{report}"
    );
    assert!(
        report.contains("generated::preserves_panic_payload ... ok"),
        "{report}"
    );

    // Select one generated case using the real libtest filter, with the same broken backend.
    let filtered = child(&["generated::returns_output", "--exact"], true);
    assert!(filtered.status.success());
    let report = String::from_utf8(filtered.stdout).unwrap();
    assert!(report.contains("1 passed; 0 failed"), "{report}");
}
