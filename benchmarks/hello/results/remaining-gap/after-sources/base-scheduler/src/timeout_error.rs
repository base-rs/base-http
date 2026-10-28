// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::OperationError;
use std::fmt;

/// Failure of a clock timeout, separate from the wrapped operation's own output.
#[derive(Debug)]
pub enum TimeoutError {
    /// The operation was still pending when its timer completed.
    Elapsed,
    /// The timer failed before the operation completed.
    Clock(OperationError),
}

impl fmt::Display for TimeoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Elapsed => formatter.write_str("operation timed out"),
            Self::Clock(error) => write!(formatter, "timeout clock failed: {error}"),
        }
    }
}

impl std::error::Error for TimeoutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Elapsed => None,
            Self::Clock(error) => Some(error),
        }
    }
}
