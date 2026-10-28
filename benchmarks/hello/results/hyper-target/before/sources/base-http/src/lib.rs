// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

pub mod body;
pub mod fields;
pub mod interim;
pub mod method;
pub mod protocol;
pub mod request;
pub mod response;
pub mod status;
pub mod version;

mod future;
mod syntax;

pub use fields::{Field, FieldError, Fields};
pub use interim::{InterimError, InterimHandler, InterimResponses};
pub use method::Method;
pub use protocol::Protocol;
pub use request::{Request, RequestHead};
pub use response::{Response, ResponseHead};
pub use status::Status;
pub use version::Version;
