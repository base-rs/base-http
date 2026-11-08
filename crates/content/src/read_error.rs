// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::Status;
use base_web_content::Rejection;
use std::{error::Error, fmt};

/// Why a request's content was not read, with any failures to remove what was stored of its uploads.
///
/// Neither its `Display` nor its `Debug` output includes submitted content, filenames, or media types, so it is safe to log, although the body and store errors it holds are their own types', with their own guarantees.
#[derive(Debug)]
#[non_exhaustive]
pub struct ReadError<B, S> {
    /// What went wrong.
    pub kind: ReadErrorKind<B, S>,
    /// The failures to remove what was stored of uploads, while cleaning up after `kind`. Empty when cleanup succeeded or nothing was stored.
    pub cleanup: Vec<S>,
}

/// The kind of [`ReadError`], where `B` is the body's error and `S` the [`UploadStore`](crate::UploadStore)'s.
#[derive(Debug)]
#[non_exhaustive]
pub enum ReadErrorKind<B, S> {
    /// The request has more than one `Content-Type` field, or one that is not a valid media type. The body was not read. 400 Bad Request.
    InvalidMediaType,
    /// The request's length exceeds the largest body the declaration could need in its format. The body was not read. 413 Content Too Large.
    ContentTooLarge {
        /// The request's length, from its body or its `Content-Length`.
        length: u64,
        /// The largest body the declaration could need, [`Content::body_size_limit`](base_web_content::Content::body_size_limit).
        limit: u64,
    },
    /// The content was refused: a fatal error, whose own status is 400, 413, or 415, or violations, which are 422 Unprocessable Content.
    Rejected(Rejection),
    /// Reading the request's body failed, including when its read was cancelled. 400 Bad Request, although after cancellation, such as when the server is shutting down, there is usually no one to respond to.
    Body(B),
    /// Storing an upload failed, including when a store operation was cancelled. 500 Internal Server Error.
    Store(S),
}

impl<B, S> ReadError<B, S> {
    pub(crate) fn new(kind: ReadErrorKind<B, S>) -> Self {
        Self {
            kind,
            cleanup: Vec::new(),
        }
    }

    /// The status a server should respond with.
    pub fn status(&self) -> Status {
        match &self.kind {
            ReadErrorKind::InvalidMediaType | ReadErrorKind::Body(_) => Status::BAD_REQUEST,
            ReadErrorKind::ContentTooLarge { .. } => Status::CONTENT_TOO_LARGE,
            ReadErrorKind::Rejected(Rejection::Error(error)) => {
                Status::new(error.status()).unwrap_or(Status::BAD_REQUEST)
            }
            ReadErrorKind::Rejected(Rejection::Violations(_)) => Status::UNPROCESSABLE_CONTENT,
            ReadErrorKind::Store(_) => Status::INTERNAL_SERVER_ERROR,
        }
    }
}

impl<B, S> fmt::Display for ReadError<B, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let described = match &self.kind {
            ReadErrorKind::InvalidMediaType => {
                formatter.write_str("request has an invalid Content-Type")
            }
            ReadErrorKind::ContentTooLarge { length, limit } => write!(
                formatter,
                "request length {length} exceeds the largest body its declaration accepts, {limit}"
            ),
            ReadErrorKind::Rejected(rejection) => rejection.fmt(formatter),
            ReadErrorKind::Body(_) => formatter.write_str("failed to read the request body"),
            ReadErrorKind::Store(_) => formatter.write_str("failed to store an upload"),
        };
        described.and_then(|()| match self.cleanup.len() {
            0 => Ok(()),
            1 => formatter.write_str(", and 1 failure to remove a stored upload"),
            count => write!(formatter, ", and {count} failures to remove stored uploads"),
        })
    }
}

/// The body or store error, for `Body` and `Store`. Cleanup failures are in [`ReadError::cleanup`].
impl<B: Error + 'static, S: Error + 'static> Error for ReadError<B, S> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ReadErrorKind::Body(error) => Some(error),
            ReadErrorKind::Store(error) => Some(error),
            _ => None,
        }
    }
}
