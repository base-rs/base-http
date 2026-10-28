// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

pub mod body;
pub mod fields;
pub mod headers;
pub mod interim;
pub mod method;
pub mod protocol;
pub mod request;
pub mod response;
pub mod status;
pub mod target;
pub mod version;

mod future;
mod syntax;

pub use fields::{Field, FieldError, Fields};
pub use headers::HttpHeaders;
pub use interim::{InterimError, InterimHandler, InterimResponses};
pub use method::Method;
pub use protocol::Protocol;
pub use request::{Request, RequestMetadata};
pub use response::{Response, ResponseMetadata};
pub use status::Status;
pub use target::{Authority, Path, Scheme};
pub use version::Version;
