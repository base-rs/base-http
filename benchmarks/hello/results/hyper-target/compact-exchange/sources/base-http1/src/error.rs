// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{Cancelled, OperationError};
use std::{fmt, io};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Cancelled,
    Invalid(&'static str),
    Limit(&'static str),
    Truncated,
    LengthMismatch,
    Body(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Cancelled => f.write_str("HTTP exchange cancelled"),
            Self::Invalid(reason) => write!(f, "invalid HTTP/1 message: {reason}"),
            Self::Limit(reason) => write!(f, "HTTP/1 limit exceeded: {reason}"),
            Self::Truncated => f.write_str("truncated HTTP/1 message"),
            Self::LengthMismatch => f.write_str("body does not match its declared length"),
            Self::Body(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Body(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<Cancelled> for Error {
    fn from(_: Cancelled) -> Self {
        Self::Cancelled
    }
}
impl From<OperationError> for Error {
    fn from(error: OperationError) -> Self {
        match error {
            OperationError::Cancelled(_) => Self::Cancelled,
            OperationError::Io(error) => Self::Io(error),
        }
    }
}
impl From<base_http::FieldError> for Error {
    fn from(error: base_http::FieldError) -> Self {
        Self::Body(Box::new(error))
    }
}
