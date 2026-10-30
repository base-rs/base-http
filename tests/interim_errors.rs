// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{InterimError, InterimHandler, InterimResponses, ResponseMetadata, Status};
use base_scheduler::Cancellation;
use futures_lite::future::block_on;
use std::{convert::Infallible, error::Error as _};

#[test]
fn interim_errors_describe_each_failure_without_a_source() {
    let cases: [(InterimError<Infallible>, &str); 3] = [
        (
            InterimError::InvalidStatus(Status::OK),
            "200 is not an interim response",
        ),
        (InterimError::Finished, "interim responses have finished"),
        (InterimError::Cancelled, "interim response cancelled"),
    ];
    for (error, message) in cases {
        assert_eq!(error.to_string(), message);
        assert!(error.source().is_none());
    }
}

// A named handler that records the statuses it observes.
#[derive(Default)]
struct Recorder(Vec<Status>);

impl InterimHandler for Recorder {
    type Error = Infallible;

    async fn on_interim_response(
        &mut self,
        response: ResponseMetadata,
        cancellation: &Cancellation,
    ) -> Result<(), Infallible> {
        assert!(!cancellation.is_cancelled());
        self.0.push(response.status);
        Ok(())
    }
}

#[test]
fn into_inner_returns_the_handler_with_its_observations() {
    let mut interim = InterimResponses::new(Recorder::default());
    block_on(interim.send_interim_response(
        ResponseMetadata::new(Status::EARLY_HINTS),
        &Cancellation::never(),
    ))
    .unwrap();
    interim.finish();
    assert_eq!(interim.into_inner().0, [Status::EARLY_HINTS]);
}
