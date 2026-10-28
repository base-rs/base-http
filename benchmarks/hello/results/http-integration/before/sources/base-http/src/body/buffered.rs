// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.
// Copyright, 2020, by Bryan Powell.
// Copyright, 2025, by William T. Nelson.

use super::ReadableBody;
use base_scheduler::{Cancellation, Cancelled};

/// An in-memory, replayable sequence of chunks.
///
/// Reads clone each chunk, following Socketry's buffered body. The application
/// chooses its owner: a `Vec<u8>` yields independent data, whereas an immutable
/// shared owner can clone cheaply. Closing preserves the chunks for rewinding.
#[derive(Clone, Debug)]
pub struct BufferedBody<D = Vec<u8>> {
    chunks: Vec<D>,
    index: usize,
    length: Option<u64>,
}

impl<D: AsRef<[u8]> + Clone + Send> BufferedBody<D> {
    pub fn new(chunks: impl IntoIterator<Item = D>) -> Self {
        let chunks: Vec<D> = chunks.into_iter().collect();
        let length = chunks.iter().try_fold(0u64, |sum, chunk| {
            sum.checked_add(u64::try_from(chunk.as_ref().len()).ok()?)
        });
        Self {
            chunks,
            index: 0,
            length,
        }
    }

    /// All original chunks, including data already read.
    pub fn chunks(&self) -> &[D] {
        &self.chunks
    }

    /// Read immediately available memory without scheduling an operation.
    pub fn read_chunk(&mut self) -> Option<D> {
        let chunk = self.chunks.get(self.index)?.clone();
        self.index += 1;
        Some(chunk)
    }

    /// A buffered representation with an independent cursor at the beginning.
    pub fn buffered(&self) -> Self {
        Self {
            chunks: self.chunks.clone(),
            index: 0,
            length: self.length,
        }
    }

    /// Append a chunk, including after earlier chunks have been read or closed.
    pub fn write(&mut self, chunk: D) {
        self.length = self
            .length
            .and_then(|length| length.checked_add(u64::try_from(chunk.as_ref().len()).ok()?));
        self.chunks.push(chunk);
    }

    pub fn clear(&mut self) {
        self.chunks.clear();
        self.index = 0;
        self.length = Some(0);
    }
}

impl<D> Default for BufferedBody<D> {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            index: 0,
            length: Some(0),
        }
    }
}

impl<D: AsRef<[u8]> + Clone + Send> From<D> for BufferedBody<D> {
    fn from(chunk: D) -> Self {
        Self::new([chunk])
    }
}

impl<D: AsRef<[u8]> + Clone + Send> ReadableBody for BufferedBody<D> {
    type Data = D;
    type Error = Cancelled;

    fn is_ready(&self) -> bool {
        true
    }

    fn is_empty(&self) -> bool {
        self.index >= self.chunks.len()
    }

    fn length(&self) -> Option<u64> {
        self.length
    }

    fn is_rewindable(&self) -> bool {
        true
    }

    fn rewind(&mut self) -> bool {
        self.index = 0;
        true
    }

    fn close(&mut self, _error: Option<Self::Error>) {
        self.index = self.chunks.len();
    }

    async fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Option<Self::Data>, Self::Error> {
        cancellation.check()?;
        Ok(self.read_chunk())
    }
}
