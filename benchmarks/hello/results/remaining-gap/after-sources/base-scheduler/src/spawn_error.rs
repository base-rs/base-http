// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::fmt;

/// A task could not be registered with its scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpawnError {
    /// The scheduler has begun shutting down.
    SchedulerClosed,
    /// All task identifiers have been used.
    IdentifiersExhausted,
}

impl fmt::Display for SpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SchedulerClosed => "the scheduler is closed",
            Self::IdentifiersExhausted => "task identifiers are exhausted",
        })
    }
}

impl std::error::Error for SpawnError {}

#[cfg(test)]
mod tests;
