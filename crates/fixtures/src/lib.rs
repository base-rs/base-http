// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod body;
mod context;
mod latch;
pub mod protocol;
pub mod raw;
mod scheduler;
pub mod socket;

#[cfg(feature = "rustls")]
pub use context::SERVER_NAME;
pub use context::{ClientContext, Context, Hello, ServerContext, TEARDOWN_TIMEOUT, TEST_TIMEOUT};
pub use latch::{LATCH_TIMEOUT, Latch};
pub use raw::{RawListener, RawPeer, RawServer};
pub use scheduler::{Scheduler, native};

/// The messages of `error` and each of its sources, outermost first.
pub fn error_chain(error: &dyn std::error::Error) -> Vec<String> {
    let mut messages = vec![error.to_string()];
    let mut source = error.source();
    while let Some(error) = source {
        messages.push(error.to_string());
        source = error.source();
    }
    messages
}
