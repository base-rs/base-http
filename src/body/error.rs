// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::Cancelled;
use std::{any::Any, error, fmt, sync::Arc};

/// Failures shared by writable bodies and their producers. `Failed` reports the producer's error as its source, so callers can downcast it.
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
    /// Wrap a producer failure. A `BodyError` is returned unchanged, so a failure relayed through several bodies is reported once.
    pub fn new<E: error::Error + Send + Sync + 'static>(error: E) -> Self {
        let mut error = Some(error);
        if let Some(body) = (&mut error as &mut dyn Any).downcast_mut::<Option<Self>>() {
            return body.take().unwrap();
        }
        Self::Failed(Arc::new(error.unwrap()))
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
            Self::Failed(_) => formatter.write_str("body failed"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error as _, io};

    #[test]
    fn failed_reports_the_producer_error_as_its_source() {
        let error = BodyError::new(io::Error::from(io::ErrorKind::TimedOut));
        assert_eq!(error.to_string(), "body failed");
        assert_eq!(error.source().unwrap().to_string(), "timed out");
    }

    #[test]
    fn wrapping_a_body_error_returns_it_unchanged() {
        let error = BodyError::new(BodyError::new(io::Error::other("producer failed")));
        assert_eq!(error.to_string(), "body failed");
        assert_eq!(error.source().unwrap().to_string(), "producer failed");
        assert!(matches!(
            BodyError::new(BodyError::Closed),
            BodyError::Closed
        ));
    }
}
