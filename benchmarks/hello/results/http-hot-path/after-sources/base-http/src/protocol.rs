// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::syntax::is_token;
use std::{error::Error, fmt, str::FromStr};

/// Application protocol intent (e.g. `websocket`), independent of HTTP version.
/// A driver translates this into Upgrade or extended CONNECT as appropriate.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Protocol(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidProtocol;

impl fmt::Display for InvalidProtocol {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("invalid application protocol token")
    }
}
impl Error for InvalidProtocol {}

impl Protocol {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidProtocol> {
        let value = value.into();
        if !is_token(value.as_bytes()) {
            return Err(InvalidProtocol);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl FromStr for Protocol {
    type Err = InvalidProtocol;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}
impl fmt::Display for Protocol {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.as_str())
    }
}
