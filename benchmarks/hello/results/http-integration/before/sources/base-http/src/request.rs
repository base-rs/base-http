// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

use crate::{Fields, Method, Protocol, Version, body::EmptyBody};
use std::ops::{Deref, DerefMut};

/// Protocol-independent request head. Components are preserved as supplied;
/// drivers validate target combinations and wire syntax before transmission.
#[derive(Clone, Debug)]
pub struct RequestHead {
    pub method: Method,
    pub scheme: Option<String>,
    pub authority: Option<String>,
    /// Path and optional query, or the supplied request-target form. `None`
    /// represents a request such as traditional CONNECT with no path.
    pub path: Option<String>,
    pub version: Option<Version>,
    pub fields: Fields,
    pub protocol: Option<Protocol>,
}

impl RequestHead {
    pub fn new(method: Method) -> Self {
        Self {
            method,
            scheme: None,
            authority: None,
            path: None,
            version: None,
            fields: Fields::new(),
            protocol: None,
        }
    }
}

/// A semantic request with an arbitrary body owner. The head is also accessible
/// through dereferencing, e.g. `request.fields` and `request.method`.
#[derive(Clone, Debug)]
pub struct Request<B = EmptyBody> {
    pub head: RequestHead,
    pub body: B,
}

impl<B> Request<B> {
    pub fn new(method: Method, path: impl Into<String>, body: B) -> Self {
        let mut head = RequestHead::new(method);
        head.path = Some(path.into());
        Self { head, body }
    }

    pub fn from_parts(head: RequestHead, body: B) -> Self {
        Self { head, body }
    }
    pub fn into_parts(self) -> (RequestHead, B) {
        (self.head, self.body)
    }
    /// Move the body through `map` without cloning or rebuilding the head.
    pub fn map_body<T>(self, map: impl FnOnce(B) -> T) -> Request<T> {
        Request {
            head: self.head,
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
    pub fn get(path: impl Into<String>) -> Self {
        Self::new(Method::GET, path, EmptyBody)
    }
}

impl<B> Deref for Request<B> {
    type Target = RequestHead;
    fn deref(&self) -> &Self::Target {
        &self.head
    }
}
impl<B> DerefMut for Request<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.head
    }
}

impl<B: crate::body::ReadableBody> Request<B> {
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
