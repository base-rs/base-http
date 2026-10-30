// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::chain;
use base_http_client::{Client, Endpoint, Error, Options};
use base_scheduler_direct::DirectScheduler;

#[test]
fn invalid_limits_are_reported_by_the_layer_that_owns_them() {
    assert!(Options::default().validate().is_ok());
    let pool = Options {
        pool: base_pool::Options {
            max_resources: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let error = pool.validate().unwrap_err();
    assert!(matches!(
        error,
        Error::Pool(base_pool::Error::InvalidOptions)
    ));
    assert_eq!(
        chain(&error),
        ["resource and construction limits must be positive"]
    );
    let http1 = Options {
        http1: base_http1::Options {
            read_size: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let error = http1.validate().unwrap_err();
    assert!(matches!(error, Error::Protocol(_)));
    assert_eq!(
        chain(&error),
        ["invalid HTTP/1 message: invalid HTTP/1 limits"]
    );
    let defaults = Options::default();
    let client = [
        Options {
            max_requests: 0,
            ..defaults.clone()
        },
        Options {
            response_buffer_frames: 0,
            ..defaults.clone()
        },
        // The response buffer must hold a complete head.
        Options {
            response_buffer_bytes: defaults.http1.max_head_bytes - 1,
            ..defaults.clone()
        },
    ];
    for options in client {
        assert!(matches!(options.validate(), Err(Error::InvalidOptions)));
    }
}

#[test]
fn clients_reject_invalid_options_before_construction() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let endpoint = Endpoint::new("127.0.0.1:1".parse().unwrap());
    let options = Options {
        max_requests: 0,
        ..Default::default()
    };
    assert!(matches!(
        Client::new(&scheduler, endpoint, options),
        Err(Error::InvalidOptions)
    ));
}

#[test]
fn clients_report_their_endpoint() {
    crate::unconnected(async |client| {
        let endpoint = client.endpoint();
        assert_eq!(endpoint.address, "127.0.0.1:1".parse().unwrap());
        assert_eq!(endpoint.authority.as_str(), "127.0.0.1:1");
        assert_eq!(endpoint.scheme(), "http");
    });
}
