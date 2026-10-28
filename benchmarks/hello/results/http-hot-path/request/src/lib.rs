// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! HTTP/1 framing over independently owned scheduler I/O lanes.
//! No runtime, tasks or transport ownership are hidden inside this crate.

#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

mod client;
mod error;
mod input;
mod output;
mod wire;

pub use client::{RequestWriter, ResponseReader};
pub use error::Error;
pub use input::{Exchange, IncomingBody, RequestReader};
pub use output::{InterimWriter, ResponseWriter};

/// Bounds apply before allocating or accepting an entire wire section.
#[derive(Clone, Debug)]
pub struct Options {
    pub max_head_bytes: usize,
    pub max_fields: usize,
    pub max_chunk_line_bytes: usize,
    pub read_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_head_bytes: 32 * 1024,
            max_fields: 100,
            max_chunk_line_bytes: 4096,
            read_size: 16 * 1024,
        }
    }
}

impl Options {
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_head_bytes < 4
            || self.max_fields == 0
            || self.max_chunk_line_bytes < 2
            || self.read_size == 0
        {
            return Err(Error::Invalid("invalid HTTP/1 limits"));
        }
        Ok(())
    }
}
