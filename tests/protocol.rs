// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{Protocol, protocol::InvalidProtocol};

#[test]
fn parsing_accepts_tokens_and_display_preserves_them() {
    let protocol: Protocol = "websocket".parse().unwrap();
    assert_eq!(protocol, Protocol::new("websocket").unwrap());
    assert_eq!(protocol.to_string(), "websocket");
    assert_eq!("h2c".parse::<Protocol>().unwrap().to_string(), "h2c");
    assert_eq!("".parse::<Protocol>(), Err(InvalidProtocol));
    assert_eq!("web socket".parse::<Protocol>(), Err(InvalidProtocol));
    assert_eq!(
        InvalidProtocol.to_string(),
        "invalid application protocol token"
    );
}
