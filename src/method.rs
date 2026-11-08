// Released under the MIT License.
// Copyright, 2018-2026, by Samuel Williams.

use crate::syntax::is_token;
use std::{borrow::Cow, error::Error, fmt, str::FromStr};

/// A case-sensitive HTTP method token. Unknown methods remain representable.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Method(Cow<'static, str>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidMethod;

impl fmt::Display for InvalidMethod {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("invalid HTTP method token")
    }
}
impl Error for InvalidMethod {}

impl Method {
    pub const GET: Self = Self(Cow::Borrowed("GET"));
    pub const HEAD: Self = Self(Cow::Borrowed("HEAD"));
    pub const POST: Self = Self(Cow::Borrowed("POST"));
    pub const PUT: Self = Self(Cow::Borrowed("PUT"));
    pub const DELETE: Self = Self(Cow::Borrowed("DELETE"));
    pub const CONNECT: Self = Self(Cow::Borrowed("CONNECT"));
    pub const OPTIONS: Self = Self(Cow::Borrowed("OPTIONS"));
    pub const TRACE: Self = Self(Cow::Borrowed("TRACE"));
    pub const PATCH: Self = Self(Cow::Borrowed("PATCH"));
    pub const QUERY: Self = Self(Cow::Borrowed("QUERY"));

    pub fn new(value: impl Into<String>) -> Result<Self, InvalidMethod> {
        let value = value.into();
        if !is_token(value.as_bytes()) {
            return Err(InvalidMethod);
        }
        Ok(Self(Cow::Owned(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Semantic safety does not imply that an attempted exchange can be retried.
    pub fn is_safe(&self) -> bool {
        matches!(
            self.as_str(),
            "GET" | "HEAD" | "OPTIONS" | "TRACE" | "QUERY"
        )
    }

    /// Method semantics only; processing state and body replay are separate.
    /// Unknown extension methods conservatively return false.
    pub fn is_idempotent(&self) -> bool {
        self.is_safe() || matches!(self.as_str(), "PUT" | "DELETE")
    }
}

impl FromStr for Method {
    type Err = InvalidMethod;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}
impl AsRef<str> for Method {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl fmt::Display for Method {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.as_str())
    }
}
