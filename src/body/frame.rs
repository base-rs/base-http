// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Fields;

/// One owned body event. Initial message fields live in the message head;
/// `Headers` carries trailing fields, after which no more data is permitted.
///
/// `end` completes this body direction after delivering the payload. `End`
/// represents completion without a payload, including completion learned after
/// the last data or fields were delivered. It never requires an empty `D`.
/// An empty data payload with `end: false` is ordinary data, not completion.
#[derive(Clone, Debug)]
pub enum BodyFrame<D> {
    Data { data: D, end: bool },
    Headers { fields: Fields, end: bool },
    End,
}

impl<D> BodyFrame<D> {
    pub const fn is_end(&self) -> bool {
        match self {
            Self::Data { end, .. } | Self::Headers { end, .. } => *end,
            Self::End => true,
        }
    }

    /// Move a data owner through an adapter, preserving fields and completion.
    pub fn map_data<T>(self, map: impl FnOnce(D) -> T) -> BodyFrame<T> {
        match self {
            Self::Data { data, end } => BodyFrame::Data {
                data: map(data),
                end,
            },
            Self::Headers { fields, end } => BodyFrame::Headers { fields, end },
            Self::End => BodyFrame::End,
        }
    }
}
