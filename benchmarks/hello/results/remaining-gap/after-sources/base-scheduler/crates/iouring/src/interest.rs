// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Interest {
    Readable,
    Writable,
}
