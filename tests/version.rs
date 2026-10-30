// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::Version;

#[test]
fn display_writes_the_protocol_version() {
    assert_eq!(Version::Http10.to_string(), "HTTP/1.0");
    assert_eq!(Version::Http11.to_string(), "HTTP/1.1");
    assert_eq!(Version::Http2.to_string(), "HTTP/2");
    assert_eq!(Version::Http3.to_string(), "HTTP/3");
}
