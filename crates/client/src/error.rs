// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{fmt, io, sync::Arc};

/// A client failure. Errors delivered to a response body retain the same cause.
#[derive(Clone, Debug)]
pub enum Error {
    Negotiation(base_http_protocol::NegotiationError),
    Closed,
    Busy,
    Cancelled,
    Abandoned,
    InvalidOptions,
    Unsupported(&'static str),
    Pool(base_pool::Error),
    Protocol(Arc<base_http1::Error>),
    Io(Arc<io::Error>),
    Body(base_http::body::BodyError),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Negotiation(error) => error.fmt(f),
            Self::Closed => f.write_str("HTTP client is closed"),
            Self::Busy => f.write_str("HTTP client request limit reached"),
            Self::Cancelled => f.write_str("HTTP client request cancelled"),
            Self::Abandoned => f.write_str("HTTP client response abandoned"),
            Self::InvalidOptions => f.write_str("invalid HTTP client limits"),
            Self::Unsupported(reason) => write!(f, "unsupported HTTP client operation: {reason}"),
            Self::Pool(error) => error.fmt(f),
            Self::Protocol(error) => error.fmt(f),
            Self::Io(error) => error.fmt(f),
            Self::Body(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Negotiation(error) => Some(error),
            Self::Pool(error) => Some(error),
            Self::Protocol(error) => Some(error.as_ref()),
            Self::Io(error) => Some(error.as_ref()),
            Self::Body(error) => Some(error),
            _ => None,
        }
    }
}
impl From<base_pool::Error> for Error {
    fn from(error: base_pool::Error) -> Self {
        match error {
            base_pool::Error::Cancelled => Self::Cancelled,
            base_pool::Error::Closed => Self::Closed,
            error => Self::Pool(error),
        }
    }
}
impl From<base_http1::Error> for Error {
    fn from(error: base_http1::Error) -> Self {
        match error {
            base_http1::Error::Cancelled => Self::Cancelled,
            error => Self::Protocol(Arc::new(error)),
        }
    }
}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}
impl From<base_scheduler::OperationError> for Error {
    fn from(error: base_scheduler::OperationError) -> Self {
        base_http1::Error::from(error).into()
    }
}
impl From<base_http::body::BodyError> for Error {
    fn from(error: base_http::body::BodyError) -> Self {
        Self::Body(error)
    }
}

impl From<base_http_protocol::NegotiationError> for Error {
    fn from(error: base_http_protocol::NegotiationError) -> Self {
        Self::Negotiation(error)
    }
}
