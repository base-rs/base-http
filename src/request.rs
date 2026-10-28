// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

use crate::{Authority, Fields, Method, Path, Protocol, Scheme, Version, body::EmptyBody};
use std::ops::{Deref, DerefMut};

/// Protocol-independent request metadata. Components are preserved as supplied,
/// except that schemes are stored lowercase; drivers validate target
/// combinations and wire syntax before transmission.
#[derive(Clone, Debug)]
pub struct RequestMetadata {
    pub method: Method,
    pub scheme: Option<Scheme>,
    pub authority: Option<Authority>,
    /// Path and optional query, or the supplied request-target form. `None`
    /// represents a request such as traditional CONNECT with no path.
    pub path: Option<Path>,
    pub version: Option<Version>,
    pub fields: Fields,
    /// Declared representation length, independent of whether this message has
    /// readable data (for example HEAD and 304 responses).
    pub content_length: Option<u64>,
    /// Outgoing connection reuse preference. Received hop-specific decisions
    /// are retained by the protocol exchange, rather than forwarded as fields.
    pub persistent: Option<bool>,
    pub protocol: Option<Protocol>,
}

impl RequestMetadata {
    pub fn new(method: Method) -> Self {
        Self {
            method,
            scheme: None,
            authority: None,
            path: None,
            version: None,
            fields: Fields::new(),
            content_length: None,
            persistent: None,
            protocol: None,
        }
    }
}

/// A semantic request with an arbitrary body owner. The metadata is also accessible
/// through dereferencing, e.g. `request.fields` and `request.method`.
#[derive(Clone, Debug)]
pub struct Request<B = EmptyBody> {
    pub metadata: RequestMetadata,
    pub body: B,
}

impl<B> Request<B> {
    pub fn new(method: Method, path: impl Into<Path>, body: B) -> Self {
        let mut metadata = RequestMetadata::new(method);
        metadata.path = Some(path.into());
        Self { metadata, body }
    }

    pub fn from_parts(metadata: RequestMetadata, body: B) -> Self {
        Self { metadata, body }
    }
    pub fn into_parts(self) -> (RequestMetadata, B) {
        (self.metadata, self.body)
    }
    /// Move the body through `map` without cloning or rebuilding the metadata.
    pub fn map_body<T>(self, map: impl FnOnce(B) -> T) -> Request<T> {
        Request {
            metadata: self.metadata,
            body: map(self.body),
        }
    }
    /// Replace and drop the previous body. This does not drain it or establish
    /// asynchronous exchange cleanup; an I/O driver retains that responsibility.
    pub fn with_body<T>(self, body: T) -> Request<T> {
        self.map_body(|_| body)
    }
    pub fn is_head(&self) -> bool {
        self.method == Method::HEAD
    }
    pub fn is_connect(&self) -> bool {
        self.method == Method::CONNECT
    }
}

impl Request<EmptyBody> {
    pub fn get(path: impl Into<Path>) -> Self {
        Self::new(Method::GET, path, EmptyBody)
    }
}

impl<B> Deref for Request<B> {
    type Target = RequestMetadata;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}
impl<B> DerefMut for Request<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metadata
    }
}

impl<B: crate::body::ReadableBody> Request<B> {
    /// Read an owned body frame. Received fields stay in the frame; the caller
    /// can forward them or explicitly append them with `HttpHeaders::append_trailers`.
    pub async fn read(
        &mut self,
        cancellation: &base_scheduler::Cancellation,
    ) -> Result<crate::body::BodyFrame<B::Data>, B::Error> {
        self.body.read(cancellation).await
    }
}
