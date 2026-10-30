// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::body::BodyError;
use base_scheduler::Cancelled;
use std::{error::Error as _, io};

#[test]
fn body_errors_describe_each_failure() {
    let cases = [
        (BodyError::from(Cancelled), "body operation cancelled"),
        (BodyError::Closed, "body output is closed"),
        (BodyError::Abandoned, "body abandoned before completion"),
        (
            BodyError::InvalidFrame,
            "body data or headers after trailing headers",
        ),
        (
            BodyError::ChunkTooLarge {
                length: 5,
                limit: 4,
            },
            "body frame has 5 bytes, exceeding queue limit 4",
        ),
    ];
    for (error, message) in cases {
        assert_eq!(error.to_string(), message);
        assert!(error.source().is_none());
    }
}

#[test]
fn io_errors_convert_into_downcastable_failures() {
    let error = BodyError::from(io::Error::new(io::ErrorKind::BrokenPipe, "peer closed"));
    assert_eq!(error.to_string(), "body failed");
    let source = error.source().unwrap().downcast_ref::<io::Error>().unwrap();
    assert_eq!(source.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(source.to_string(), "peer closed");
}
