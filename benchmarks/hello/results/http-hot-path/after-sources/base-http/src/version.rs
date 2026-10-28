// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::fmt;

/// The negotiated or received HTTP version. Message heads use `None` before
/// selection; protocol selection policy is separate from this value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Version {
    Http10,
    Http11,
    Http2,
    Http3,
}

impl fmt::Display for Version {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(match self {
            Self::Http10 => "HTTP/1.0",
            Self::Http11 => "HTTP/1.1",
            Self::Http2 => "HTTP/2",
            Self::Http3 => "HTTP/3",
        })
    }
}
