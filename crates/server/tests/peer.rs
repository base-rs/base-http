// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Handlers observe the connection a request arrived on through their context.

use base_http::{
    Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_server::{Handler, ServerContext};
use base_scheduler::Cancellation;
use std::io::{self, Write};

mod support;
use support::*;

struct Reporting;
impl<'h> Handler<'h, ServerContext> for Reporting {
    type Error = io::Error;
    type Body<B: ReadableBody + 'h> = BufferedBody;
    async fn call<B: ReadableBody + 'h>(
        &self,
        _: Request<B>,
        context: &mut ServerContext,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        let peer = context.peer();
        let mut response = Response::new(Status::OK, BufferedBody::default());
        for (name, value) in [
            ("x-peer", peer.address().map(|address| address.to_string())),
            (
                "x-local",
                peer.local_address().map(|address| address.to_string()),
            ),
            ("x-encrypted", Some(peer.is_encrypted().to_string())),
            ("x-early-data", Some(peer.is_early_data().to_string())),
        ] {
            response
                .fields
                .add(name, value.unwrap_or_default().as_str())
                .unwrap();
        }
        Ok(response)
    }
}

#[test]
fn handlers_observe_the_peer_and_local_addresses_of_their_connection() {
    let (served, (expected, response)) = connection(Reporting, |mut peer, _| {
        let socket = peer.get_ref();
        let expected = (socket.local_addr().unwrap(), socket.peer_addr().unwrap());
        peer.get_mut()
            .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        (expected, head(&mut peer))
    });
    assert_eq!(served.unwrap(), 1);
    assert_eq!(response.status, 200);
    let (address, local) = expected;
    assert_eq!(response.get("x-peer"), Some(address.to_string().as_str()));
    assert_eq!(response.get("x-local"), Some(local.to_string().as_str()));
    assert_eq!(response.get("x-encrypted"), Some("false"));
    assert_eq!(response.get("x-early-data"), Some("false"));
}
