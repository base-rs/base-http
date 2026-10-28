// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::Cancelled;
use std::{error, fmt, sync::Arc};

/// Failures shared by writable bodies and their producers.
#[derive(Clone, Debug)]
pub enum BodyError {
    Cancelled,
    Closed,
    Abandoned,
    InvalidFrame,
    ChunkTooLarge { length: usize, limit: usize },
    Failed(Arc<dyn error::Error + Send + Sync>),
}

impl BodyError {
    pub fn new(error: impl error::Error + Send + Sync + 'static) -> Self {
        Self::Failed(Arc::new(error))
    }
}

impl From<Cancelled> for BodyError {
    fn from(_: Cancelled) -> Self {
        Self::Cancelled
    }
}

impl From<std::io::Error> for BodyError {
    fn from(error: std::io::Error) -> Self {
        Self::new(error)
    }
}

impl fmt::Display for BodyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("body operation cancelled"),
            Self::Closed => formatter.write_str("body output is closed"),
            Self::Abandoned => formatter.write_str("body abandoned before completion"),
            Self::InvalidFrame => {
                formatter.write_str("body data or headers after trailing headers")
            }
            Self::ChunkTooLarge { length, limit } => write!(
                formatter,
                "body frame has {length} bytes, exceeding queue limit {limit}"
            ),
            Self::Failed(error) => error.fmt(formatter),
        }
    }
}

impl error::Error for BodyError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Failed(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}
