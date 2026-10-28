// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

use crate::{Fields, Protocol, Status, Version, body::EmptyBody};
use std::ops::{Deref, DerefMut};

/// Informational and final responses share the same head. Protocol/responder
/// capabilities sequence heads; upgrades and CONNECT use bidirectional bodies,
/// with resource lifecycle retained by the exchange/driver.
#[derive(Clone, Debug)]
pub struct ResponseHead {
    pub status: Status,
    pub version: Option<Version>,
    pub fields: Fields,
    pub protocol: Option<Protocol>,
}

impl ResponseHead {
    pub fn new(status: Status) -> Self {
        Self {
            status,
            version: None,
            fields: Fields::new(),
            protocol: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Response<B = EmptyBody> {
    pub head: ResponseHead,
    pub body: B,
}

impl<B> Response<B> {
    pub fn new(status: Status, body: B) -> Self {
        Self {
            head: ResponseHead::new(status),
            body,
        }
    }
    pub fn from_parts(head: ResponseHead, body: B) -> Self {
        Self { head, body }
    }
    pub fn into_parts(self) -> (ResponseHead, B) {
        (self.head, self.body)
    }
    /// Move the body through `map` without cloning or rebuilding the head.
    pub fn map_body<T>(self, map: impl FnOnce(B) -> T) -> Response<T> {
        Response {
            head: self.head,
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
    type Target = ResponseHead;
    fn deref(&self) -> &Self::Target {
        &self.head
    }
}
impl<B> DerefMut for Response<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.head
    }
}

impl<B: crate::body::ReadableBody> Response<B> {
    /// Read data and append any received trailers to this message's fields at EOF.
    pub async fn read(
        &mut self,
        cancellation: &base_scheduler::Cancellation,
    ) -> Result<Option<B::Data>, B::Error> {
        let chunk = self.body.read(cancellation).await?;
        if chunk.is_none() {
            self.body.finish(&mut self.head.fields)?;
        }
        Ok(chunk)
    }
}
