// Released under the MIT License.
// Copyright, 2024-2026, by Samuel Williams.

use super::{BodyError, BodyFrame, ReadableBody, WritableBody};
use crate::future::cancellable;
use base_scheduler::Cancellation;
use event_listener::Event;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

struct State<D> {
    frames: VecDeque<(BodyFrame<D>, usize)>,
    bytes: usize,
    read_closed: bool,
    write_closed: bool,
    headers: bool,
    error: Option<BodyError>,
}

pub(crate) struct Shared<D> {
    state: Mutex<State<D>>,
    readable: Event,
    writable: Event,
    max_bytes: usize,
    max_chunks: usize,
    length: Option<u64>,
}

impl<D> Shared<D> {
    fn lock(&self) -> MutexGuard<'_, State<D>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // The unique writer calls this at most once, from `close_write` or its drop, and never after accepting a terminal frame, so output is still open here.
    pub(crate) fn finish(&self, mut error: Option<BodyError>) {
        let discarded = {
            let mut state = self.lock();
            let discarded = if error.is_some() {
                if state.error.is_none() {
                    state.error = error.take();
                }
                state.bytes = 0;
                std::mem::take(&mut state.frames)
            } else {
                VecDeque::new()
            };
            state.write_closed = true;
            discarded
        };
        self.readable.notify(usize::MAX);
        self.writable.notify(usize::MAX);
        drop(discarded);
    }
}

/// The readable half of a bounded, single-producer body.
///
/// Byte limits count data and field name/value bytes; item limits count every
/// frame, including empty data and headers. Oversized frames are rejected rather
/// than waiting forever. Owners may retain additional storage. Dropping/closing
/// the reader abandons queued frames and wakes the writer. A temporarily empty
/// open body is neither ready nor exhausted.
pub struct BodyReader<D = Vec<u8>> {
    shared: Arc<Shared<D>>,
}

/// The unique output half of a [`BodyReader`] body.
///
/// Finish with a terminal frame or `close_write(None)` to preserve queued frames.
/// An error closes immediately and discards queued frames. Dropping unfinished
/// output reports abandonment. A cancelled/pending write has not queued its
/// frame; the supplied owner is dropped when that write future is dropped.
pub struct BodyWriter<D = Vec<u8>> {
    shared: Arc<Shared<D>>,
    closed: bool,
}

impl<D: AsRef<[u8]> + Send> BodyReader<D> {
    /// Create independently movable input/output halves. Panics for zero limits.
    pub fn bounded(max_bytes: usize, max_chunks: usize) -> (Self, BodyWriter<D>) {
        Self::pair(max_bytes, max_chunks, None)
    }

    /// Declare total representation length; protocol framing checks actual size.
    pub fn with_length(max_bytes: usize, max_chunks: usize, length: u64) -> (Self, BodyWriter<D>) {
        Self::pair(max_bytes, max_chunks, Some(length))
    }

    fn pair(max_bytes: usize, max_chunks: usize, length: Option<u64>) -> (Self, BodyWriter<D>) {
        assert!(max_bytes > 0, "body byte limit must be positive");
        assert!(max_chunks > 0, "body chunk limit must be positive");
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                frames: VecDeque::new(),
                bytes: 0,
                read_closed: false,
                write_closed: false,
                headers: false,
                error: None,
            }),
            readable: Event::new(),
            writable: Event::new(),
            max_bytes,
            max_chunks,
            length,
        });
        (
            Self {
                shared: Arc::clone(&shared),
            },
            BodyWriter {
                shared,
                closed: false,
            },
        )
    }
}

impl<D: AsRef<[u8]> + Send> ReadableBody for BodyReader<D> {
    type Data = D;
    type Error = BodyError;

    fn is_ready(&self) -> bool {
        let state = self.shared.lock();
        !state.frames.is_empty() || state.read_closed || state.write_closed
    }
    fn is_empty(&self) -> bool {
        let state = self.shared.lock();
        state.frames.is_empty() && (state.read_closed || state.write_closed)
    }
    fn length(&self) -> Option<u64> {
        self.shared.length
    }
    fn close(&mut self, error: Option<BodyError>) {
        let discarded = {
            let mut state = self.shared.lock();
            state.read_closed = true;
            state.bytes = 0;
            if state.error.is_none() {
                state.error = error;
            }
            std::mem::take(&mut state.frames)
        };
        self.shared.readable.notify(usize::MAX);
        self.shared.writable.notify(usize::MAX);
        drop(discarded);
    }

    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<D>, BodyError> {
        loop {
            cancellation.check()?;
            // Register before examining the condition to avoid lost wakeups.
            event_listener::listener!(self.shared.readable => listener);
            let chunk = {
                let mut state = self.shared.lock();
                if let Some(error) = &state.error {
                    return Err(error.clone());
                }
                if let Some((mut frame, length)) = state.frames.pop_front() {
                    state.bytes -= length;
                    if state.write_closed && state.frames.is_empty() {
                        match &mut frame {
                            BodyFrame::Data { end, .. } | BodyFrame::Headers { end, .. } => {
                                *end = true
                            }
                            BodyFrame::End => {}
                        }
                    }
                    Some(frame)
                } else if state.write_closed || state.read_closed {
                    return Ok(BodyFrame::End);
                } else {
                    None
                }
            };
            if let Some(chunk) = chunk {
                self.shared.writable.notify(usize::MAX);
                return Ok(chunk);
            }
            cancellable(listener, cancellation).await?;
        }
    }
}

impl<D: AsRef<[u8]> + Send> BodyWriter<D> {
    pub async fn write(&mut self, data: D, cancellation: &Cancellation) -> Result<(), BodyError> {
        self.write_frame(BodyFrame::Data { data, end: false }, cancellation)
            .await
    }

    /// Move a whole frame into the queue. Headers must follow all data; an end
    /// flag or `End` closes output atomically with accepting that frame.
    /// Capacity counts data bytes and field name/value bytes, plus every frame
    /// against the item limit. As with data, owners may retain additional memory.
    pub async fn write_frame(
        &mut self,
        frame: BodyFrame<D>,
        cancellation: &Cancellation,
    ) -> Result<(), BodyError> {
        cancellation.check()?;
        let length = match &frame {
            BodyFrame::Data { data, .. } => data.as_ref().len(),
            BodyFrame::Headers { fields, .. } => {
                fields.pairs().iter().fold(0usize, |sum, field| {
                    sum.saturating_add(field.name().len())
                        .saturating_add(field.value().len())
                })
            }
            BodyFrame::End => 0,
        };
        if length > self.shared.max_bytes {
            return Err(BodyError::ChunkTooLarge {
                length,
                limit: self.shared.max_bytes,
            });
        }
        loop {
            event_listener::listener!(self.shared.writable => listener);
            {
                let mut state = self.shared.lock();
                if let Some(error) = &state.error {
                    return Err(error.clone());
                }
                if state.read_closed {
                    return Err(BodyError::Abandoned);
                }
                if state.write_closed {
                    return Err(BodyError::Closed);
                }
                if state.headers && !matches!(&frame, BodyFrame::End) {
                    return Err(BodyError::InvalidFrame);
                }
                if state.frames.len() < self.shared.max_chunks
                    && state.bytes <= self.shared.max_bytes - length
                {
                    state.headers |= matches!(&frame, BodyFrame::Headers { .. });
                    let end = frame.is_end();
                    state.bytes += length;
                    state.frames.push_back((frame, length));
                    if end {
                        state.write_closed = true;
                        self.closed = true;
                    }
                    drop(state);
                    self.shared.readable.notify(usize::MAX);
                    return Ok(());
                }
            }
            cancellable(listener, cancellation).await?;
        }
    }

    pub async fn flush(&mut self, cancellation: &Cancellation) -> Result<(), BodyError> {
        cancellation.check()?;
        let state = self.shared.lock();
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        if state.read_closed {
            return Err(BodyError::Abandoned);
        }
        // Queued chunks are already visible to the consumer. The protocol
        // writer, rather than this queue, owns transport flushing.
        Ok(())
    }

    pub fn close_write(&mut self, error: Option<BodyError>) {
        if !self.closed {
            self.closed = true;
            self.shared.finish(error);
        }
    }

    pub fn abort(&mut self) {
        self.close_write(Some(BodyError::Abandoned));
    }
}

impl<D> Drop for BodyReader<D> {
    fn drop(&mut self) {
        let discarded = {
            let mut state = self.shared.lock();
            state.read_closed = true;
            state.bytes = 0;
            std::mem::take(&mut state.frames)
        };
        self.shared.writable.notify(usize::MAX);
        drop(discarded);
    }
}

impl<D> Drop for BodyWriter<D> {
    fn drop(&mut self) {
        if !self.closed {
            self.shared.finish(Some(BodyError::Abandoned));
        }
    }
}

impl<D: AsRef<[u8]> + Send> WritableBody for BodyWriter<D> {
    type Data = D;
    type Error = BodyError;
    async fn write_frame(
        &mut self,
        frame: BodyFrame<D>,
        cancellation: &Cancellation,
    ) -> Result<(), BodyError> {
        BodyWriter::write_frame(self, frame, cancellation).await
    }
    async fn write(&mut self, data: D, cancellation: &Cancellation) -> Result<(), BodyError> {
        BodyWriter::write(self, data, cancellation).await
    }
    async fn flush(&mut self, cancellation: &Cancellation) -> Result<(), BodyError> {
        BodyWriter::flush(self, cancellation).await
    }
    fn close_write(&mut self, error: Option<BodyError>) {
        BodyWriter::close_write(self, error)
    }
    fn abort(&mut self) {
        BodyWriter::abort(self)
    }
}
