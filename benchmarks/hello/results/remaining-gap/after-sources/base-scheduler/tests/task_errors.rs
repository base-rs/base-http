// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{BlockingError, TaskError};
use std::error::Error;

#[test]
fn task_errors_preserve_panic_payloads() {
    let cancelled = TaskError::Cancelled;
    assert_eq!(cancelled.to_string(), "task cancelled");
    assert!(cancelled.source().is_none());
    let panicked = TaskError::Panicked(Box::new(String::from("original payload")));
    assert_eq!(panicked.to_string(), "task panicked");
    assert!(panicked.source().is_none());
    let TaskError::Panicked(payload) = panicked else {
        panic!("expected panic payload");
    };
    assert_eq!(*payload.downcast::<String>().unwrap(), "original payload");
}

#[test]
fn blocking_admission_errors_are_distinct_from_job_errors() {
    for (error, message) in [
        (BlockingError::Cancelled, "blocking admission cancelled"),
        (BlockingError::SchedulerClosed, "the scheduler is closed"),
    ] {
        assert_eq!(error.to_string(), message);
        assert!(error.source().is_none());
    }
    let error = BlockingError::Io(std::io::Error::other("worker creation failed"));
    assert_eq!(error.to_string(), "worker creation failed");
    assert_eq!(
        error.source().unwrap().to_string(),
        "worker creation failed"
    );
}
