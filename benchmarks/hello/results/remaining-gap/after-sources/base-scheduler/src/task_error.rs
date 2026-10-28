// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{any::Any, fmt};

/// A task ended without producing its normal output.
#[derive(Debug)]
pub enum TaskError {
    /// The future was destroyed or the blocking closure was skipped before execution.
    Cancelled,
    /// Polling a future or executing a blocking closure panicked. The original payload is retained for the caller.
    Panicked(Box<dyn Any + Send + 'static>),
}

impl fmt::Display for TaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("task cancelled"),
            Self::Panicked(_) => formatter.write_str("task panicked"),
        }
    }
}

impl std::error::Error for TaskError {}
