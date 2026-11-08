// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

use base_http::{Authority, Method, Request, Scheme, Version, body::EmptyBody};
use std::{cell::Cell, rc::Rc};

struct Tracked(Rc<Cell<bool>>);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn replacing_the_body_preserves_the_head_and_drops_the_previous_body() {
    let dropped = Rc::new(Cell::new(false));
    let mut request = Request::new(Method::POST, "/upload", Tracked(dropped.clone()));
    request.scheme = Some(Scheme::HTTPS);
    request.authority = Some(Authority::from("Example.COM"));
    request.version = Some(Version::Http11);
    request.content_length = Some(3);
    request.fields.add("Content-Type", "text/plain").unwrap();

    let request = request.with_body(EmptyBody);
    assert!(dropped.get());
    assert_eq!(request.method, Method::POST);
    assert_eq!(request.path.as_deref(), Some("/upload"));
    assert_eq!(request.scheme, Some(Scheme::HTTPS));
    assert_eq!(request.authority.as_deref(), Some("example.com"));
    assert_eq!(request.version, Some(Version::Http11));
    assert_eq!(request.content_length, Some(3));
    assert!(request.fields.contains("content-type"));
}
