// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http_client::Error;
use std::{error::Error as _, io};

fn chain(error: &dyn std::error::Error) -> Vec<String> {
    let mut messages = vec![error.to_string()];
    let mut source = error.source();
    while let Some(error) = source {
        messages.push(error.to_string());
        source = error.source();
    }
    messages
}

#[test]
fn client_states_describe_themselves_without_a_cause() {
    let errors = [
        (Error::Closed, "HTTP client is closed"),
        (Error::Busy, "HTTP client request limit reached"),
        (Error::Cancelled, "HTTP client request cancelled"),
        (Error::Abandoned, "HTTP client response abandoned"),
        (Error::InvalidOptions, "invalid HTTP client limits"),
        (
            Error::Unsupported("managed client tunnels and upgrades"),
            "unsupported HTTP client operation: managed client tunnels and upgrades",
        ),
    ];
    for (error, message) in errors {
        assert_eq!(chain(&error), [message]);
    }
}

#[test]
fn pool_cancellation_and_closure_become_client_states() {
    assert!(matches!(
        Error::from(base_pool::Error::Cancelled),
        Error::Cancelled
    ));
    assert!(matches!(
        Error::from(base_pool::Error::Closed),
        Error::Closed
    ));
    let error = Error::from(base_pool::Error::WaitersFull);
    assert!(matches!(error, Error::Pool(base_pool::Error::WaitersFull)));
    assert_eq!(chain(&error), ["pool waiter limit reached"]);
}

#[test]
fn protocol_cancellation_becomes_a_client_state() {
    assert!(matches!(
        Error::from(base_http1::Error::Cancelled),
        Error::Cancelled
    ));
    let error = Error::from(base_http1::Error::Truncated);
    assert!(matches!(error, Error::Protocol(_)));
    assert_eq!(chain(&error), ["truncated HTTP/1 message"]);
}

#[test]
fn scheduler_operation_failures_classify_as_protocol_failures() {
    let cancelled = base_scheduler::OperationError::Cancelled(base_scheduler::Cancelled);
    assert!(matches!(Error::from(cancelled), Error::Cancelled));
    let failed = base_scheduler::OperationError::Io(io::Error::from(io::ErrorKind::BrokenPipe));
    let error = Error::from(failed);
    assert!(matches!(error, Error::Protocol(_)));
    assert_eq!(
        chain(&error),
        ["HTTP/1 connection I/O failed", "broken pipe"]
    );
}

#[test]
fn io_failures_are_contextual() {
    let error = Error::from(io::Error::from(io::ErrorKind::BrokenPipe));
    assert_eq!(chain(&error), ["HTTP client I/O failed", "broken pipe"]);
    assert!(error.source().unwrap().is::<io::Error>());
}
