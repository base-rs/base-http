// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{BodyFrame, ReadableBody};
use base_scheduler::{Cancellation, Cancelled};

/// An empty body retaining the representation length for a HEAD response.
#[derive(Clone, Copy, Debug, Default)]
pub struct HeadBody {
    length: Option<u64>,
}

impl HeadBody {
    pub const fn new(length: Option<u64>) -> Self {
        Self { length }
    }

    /// Capture the representation length and close the supplied body.
    pub fn for_body<B: ReadableBody + ?Sized>(body: &mut B) -> Self {
        let head = Self::new(body.length());
        body.close(None);
        head
    }
}

impl ReadableBody for HeadBody {
    type Data = Vec<u8>;
    type Error = Cancelled;

    fn may_have_trailers(&self) -> bool {
        false
    }

    fn is_ready(&self) -> bool {
        true
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn length(&self) -> Option<u64> {
        self.length
    }

    fn close(&mut self, _error: Option<Self::Error>) {}

    async fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<BodyFrame<Self::Data>, Self::Error> {
        cancellation.check()?;
        Ok(BodyFrame::End)
    }
}
