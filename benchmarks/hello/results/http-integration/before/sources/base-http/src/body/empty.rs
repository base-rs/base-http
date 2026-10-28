// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::ReadableBody;
use base_scheduler::{Cancellation, Cancelled};

/// A ready body with no data and a representation length of zero.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyBody;

impl ReadableBody for EmptyBody {
    type Data = Vec<u8>;
    type Error = Cancelled;

    fn is_ready(&self) -> bool {
        true
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn length(&self) -> Option<u64> {
        Some(0)
    }

    fn close(&mut self, _error: Option<Self::Error>) {}

    async fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Option<Self::Data>, Self::Error> {
        cancellation.check()?;
        Ok(None)
    }
}
