// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

use crate::{Fields, Protocol, Status, Version, body::EmptyBody};
use std::ops::{Deref, DerefMut};

/// Informational and final responses share the same metadata. Protocol/responder
/// capabilities sequence heads; upgrades and CONNECT use bidirectional bodies,
/// with resource lifecycle retained by the exchange/driver.
#[derive(Clone, Debug)]
pub struct ResponseMetadata {
    pub status: Status,
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

impl ResponseMetadata {
    pub fn new(status: Status) -> Self {
        Self {
            status,
            version: None,
            fields: Fields::new(),
            content_length: None,
            persistent: None,
            protocol: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Response<B = EmptyBody> {
    pub metadata: ResponseMetadata,
    pub body: B,
}

impl<B> Response<B> {
    pub fn new(status: Status, body: B) -> Self {
        Self {
            metadata: ResponseMetadata::new(status),
            body,
        }
    }
    pub fn from_parts(metadata: ResponseMetadata, body: B) -> Self {
        Self { metadata, body }
    }
    pub fn into_parts(self) -> (ResponseMetadata, B) {
        (self.metadata, self.body)
    }
    /// Move the body through `map` without cloning or rebuilding the metadata.
    pub fn map_body<T>(self, map: impl FnOnce(B) -> T) -> Response<T> {
        Response {
            metadata: self.metadata,
            body: map(self.body),
        }
    }
    /// Replace and drop the previous body. This does not drain it or establish
    /// asynchronous exchange cleanup; an I/O driver retains that responsibility.
    pub fn with_body<T>(self, body: T) -> Response<T> {
        self.map_body(|_| body)
    }
}

impl Response<EmptyBody> {
    pub fn empty(status: Status) -> Self {
        Self::new(status, EmptyBody)
    }
}

impl<B> Deref for Response<B> {
    type Target = ResponseMetadata;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}
impl<B> DerefMut for Response<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metadata
    }
}

impl<B: crate::body::ReadableBody> Response<B> {
    /// Read an owned body frame. Received fields stay in the frame; the caller
    /// can forward them or explicitly append them with `HttpHeaders::append_trailers`.
    pub async fn read(
        &mut self,
        cancellation: &base_scheduler::Cancellation,
    ) -> Result<crate::body::BodyFrame<B::Data>, B::Error> {
        self.body.read(cancellation).await
    }
}
