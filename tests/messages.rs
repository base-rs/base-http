// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

use base_http::{
    Method, Protocol, Request, RequestMetadata, Response, Status, Version,
    body::{BufferedBody, EmptyBody},
};
use std::{cell::Cell, rc::Rc};

#[test]
fn request_body_mapping_preserves_the_entire_head_and_moves_the_body_once() {
    // Neither Clone nor a body trait is required to map an application owner.
    struct Owner(Rc<Cell<usize>>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }
    let drops = Rc::new(Cell::new(0));
    let mut request = Request::new(Method::CONNECT, "/chat?channel=a%2Fb", Owner(drops.clone()));
    request.scheme = Some("https".into());
    request.authority = Some("example.com:443".into());
    request.version = Some(Version::Http2);
    request.protocol = Some(Protocol::new("websocket").unwrap());
    request.content_length = Some(4);
    request.persistent = Some(false);
    request.fields.add("x-id", "start").unwrap();
    request.fields.add("x-id", "finish").unwrap();
    let mapped = request.map_body(|owner| {
        assert_eq!(drops.get(), 0);
        drop(owner);
        BufferedBody::from(b"body".to_vec())
    });
    assert_eq!(drops.get(), 1);
    assert!(mapped.is_connect());
    assert_eq!(mapped.scheme.as_deref(), Some("https"));
    assert_eq!(mapped.authority.as_deref(), Some("example.com:443"));
    assert_eq!(mapped.path.as_deref(), Some("/chat?channel=a%2Fb"));
    assert_eq!(mapped.version, Some(Version::Http2));
    assert_eq!(mapped.protocol.as_ref().unwrap().as_str(), "websocket");
    assert_eq!(mapped.content_length, Some(4));
    assert_eq!(mapped.persistent, Some(false));
    assert_eq!(mapped.fields.values("x-id").count(), 2);
    let (head, mut body) = mapped.into_parts();
    assert_eq!(body.read_chunk(), Some(b"body".to_vec()));
    let rebuilt = Request::from_parts(head, EmptyBody);
    assert_eq!(rebuilt.fields.len(), 2);
}

#[test]
fn target_components_do_not_normalize_origin_asterisk_or_authority_forms() {
    let request = Request::get("/a/../b%2Fc?key=1&key=2");
    assert_eq!(request.path.as_deref(), Some("/a/../b%2Fc?key=1&key=2"));
    assert_eq!(request.version, None);
    let options = Request::new(Method::OPTIONS, "*", EmptyBody);
    assert_eq!(options.path.as_deref(), Some("*"));
    let mut head = RequestMetadata::new(Method::CONNECT);
    head.authority = Some("[::1]:443".into());
    let connect = Request::from_parts(head, EmptyBody);
    assert!(connect.is_connect());
    assert_eq!(connect.path, None);
    assert_eq!(connect.scheme, None);
    assert_eq!(connect.authority.as_deref(), Some("[::1]:443"));
}

#[test]
fn response_decomposition_and_mapping_preserve_status_fields_and_protocol() {
    let mut response = Response::new(Status::SWITCHING_PROTOCOLS, "body");
    response.version = Some(Version::Http11);
    response.protocol = Some(Protocol::new("websocket").unwrap());
    response.fields.add("Upgrade", "websocket").unwrap();
    let response = response.map_body(str::len);
    let (head, length) = response.into_parts();
    assert_eq!(length, 4);
    let response = Response::from_parts(head, EmptyBody).with_body(vec![1, 2, 3]);
    assert_eq!(response.status, Status::SWITCHING_PROTOCOLS);
    assert_eq!(response.version, Some(Version::Http11));
    assert_eq!(response.protocol.as_ref().unwrap().as_str(), "websocket");
    assert!(response.fields.contains("upgrade"));
    assert!(response.status.is_final());
    assert!(response.status.is_informational());
}

#[test]
fn method_semantics_do_not_depend_on_body_presence_or_change_extension_names() {
    assert!(Method::GET.is_safe());
    assert!(Method::PUT.is_idempotent());
    assert!(!Method::PUT.is_safe());
    assert!(!Method::POST.is_idempotent());
    assert!(!Method::CONNECT.is_idempotent());
    let custom = Method::new("example!method").unwrap();
    assert_eq!(custom.as_str(), "example!method");
    assert!(!custom.is_idempotent());
    assert!(!Method::new("get").unwrap().is_safe());
    assert!(Request::new(Method::HEAD, "/", EmptyBody).is_head());
    for invalid in ["", "GET /", "GET\r\n", "méthod"] {
        assert!(Method::new(invalid).is_err());
    }
}

#[test]
fn statuses_classify_interim_final_redirects_and_unknown_codes() {
    assert!(Status::CONTINUE.is_continue());
    assert!(!Status::CONTINUE.is_final());
    assert!(Status::EARLY_HINTS.is_informational());
    assert!(!Status::EARLY_HINTS.is_final());
    assert!(Status::SWITCHING_PROTOCOLS.is_final());
    assert!(Status::OK.is_ok());
    assert!(Status::PARTIAL_CONTENT.is_success());
    assert!(Status::PARTIAL_CONTENT.is_partial());
    assert!(Status::NOT_MODIFIED.is_not_modified());
    assert!(Status::TEMPORARY_REDIRECT.preserves_method());
    assert!(!Status::SEE_OTHER.preserves_method());
    assert!(Status::BAD_REQUEST.is_client_error());
    assert!(Status::INTERNAL_SERVER_ERROR.is_server_error());
    assert!(Status::new(599).unwrap().is_failure());
    assert_eq!(Status::new(299).unwrap().description(), None);
    assert_eq!(Status::NOT_FOUND.description(), Some("Not Found"));
    for code in [0, 99, 600, 999, u16::MAX] {
        assert!(Status::new(code).is_err());
    }
}

#[test]
fn messages_remain_send_when_their_body_is_send() {
    fn require_send<T: Send>() {}
    require_send::<Request<BufferedBody>>();
    require_send::<Response<BufferedBody>>();
    assert_eq!(Version::Http3.to_string(), "HTTP/3");
    assert_eq!(Response::empty(Status::OK).status, Status::OK);
}
