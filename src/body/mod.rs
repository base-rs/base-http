// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.
// Copyright, 2023, by Bruno Sutic.

//! Native HTTP bodies, following `Protocol::HTTP::Body`.
//!
//! [`ReadableBody`] combines synchronous observations and abandonment with
//! asynchronous frame reads using explicit cancellation. I/O-backed
//! implementations retain explicitly constructed scheduler-bound capabilities.
//!
//! Body length describes the whole representation. It does not decrease as chunks
//! are consumed, and a [`HeadBody`] body may report a nonzero length while emitting no
//! data. Reads move owned [`BodyFrame`] events, including trailing fields and
//! completion. An empty data chunk is distinct from completion.
//!
//! [`BodyReader`] and [`BodyWriter`] are bounded consumer/producer halves.
//! A protocol driver pulls data directly; bodies do not spawn tasks.

use base_scheduler::Cancellation;
use std::future::Future;

mod buffered;
mod empty;
mod error;
mod frame;
mod head;
mod writable;

pub use buffered::BufferedBody;
pub use empty::EmptyBody;
pub use error::BodyError;
pub use frame::BodyFrame;
pub use head::HeadBody;
pub use writable::{BodyReader, BodyWriter};

/// Protocol-independent body observations and lifecycle.
///
/// No observation performs I/O or requires a scheduler. Readiness, exhaustion,
/// known representation length and rewindability are independent properties.
pub trait ReadableBody: Send {
    /// Application data, without imposing completion-I/O buffer requirements.
    type Data: AsRef<[u8]> + Send;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Whether another read can proceed without waiting for produced data.
    ///
    /// This is a conservative batching hint. Terminal completion/error can also
    /// be ready. It does not promise that an I/O operation will finish in one poll.
    fn is_ready(&self) -> bool {
        false
    }

    /// Whether no unread payload frames remain. False can mean unknown; an open
    /// queue with no frames is not exhausted. This does not suppress read errors.
    fn is_empty(&self) -> bool {
        false
    }

    /// Total representation length, independent of the current read position.
    fn length(&self) -> Option<u64> {
        None
    }

    /// Whether this body can emit trailing headers. HTTP/1 needs this before
    /// reading to choose chunked framing, even when the data length is known.
    /// Returning false promises that no Headers frame will be produced.
    fn may_have_trailers(&self) -> bool {
        true
    }

    fn is_rewindable(&self) -> bool {
        false
    }

    /// Restore the read position when supported, reporting success.
    fn rewind(&mut self) -> bool {
        false
    }

    /// Abandon further application reads, optionally reporting their failure.
    ///
    /// This synchronous operation does not guarantee asynchronous resource
    /// retirement. An I/O-backed implementation must leave cleanup with an
    /// explicit resource/driver owner. Cancelling or dropping a read does not
    /// prove that backend access ended, nor that a connection can be reused.
    fn close(&mut self, error: Option<Self::Error>);

    /// Read one owned frame. Pre-cancellation prevents consumption. The caller
    /// stops after `frame.is_end()`; subsequent reads return `BodyFrame::End`.
    /// Headers occur at most once, after all data. Dropping a pending read is
    /// not transport retirement, and must not lose an unreturned queued frame.
    fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<BodyFrame<Self::Data>, Self::Error>> + Send;
}
impl<R: ReadableBody + ?Sized> ReadableBody for &mut R {
    type Data = R::Data;
    type Error = R::Error;

    fn is_ready(&self) -> bool {
        (**self).is_ready()
    }
    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }
    fn length(&self) -> Option<u64> {
        (**self).length()
    }
    fn may_have_trailers(&self) -> bool {
        (**self).may_have_trailers()
    }
    fn is_rewindable(&self) -> bool {
        (**self).is_rewindable()
    }
    fn rewind(&mut self) -> bool {
        (**self).rewind()
    }
    fn close(&mut self, error: Option<Self::Error>) {
        (**self).close(error)
    }

    async fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<BodyFrame<Self::Data>, Self::Error> {
        (**self).read(cancellation).await
    }
}

/// A body producer accepting owned frames with backpressure.
/// Normal closure preserves accepted frames; failure abandons unfinished output.
/// Flushing exposes accepted frames to the consumer, without promising peer receipt.
pub trait WritableBody: Send {
    type Data: AsRef<[u8]> + Send;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Accept one whole frame. Headers follow all data; a terminal frame closes
    /// output atomically with acceptance. Later writes fail. Queue capacity
    /// becoming available does not mean the transport has released its buffers.
    fn write_frame(
        &mut self,
        frame: BodyFrame<Self::Data>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Write a nonfinal data frame. Use `write_frame` to carry fields or end.
    fn write(
        &mut self,
        data: Self::Data,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.write_frame(BodyFrame::Data { data, end: false }, cancellation)
    }
    fn flush(
        &mut self,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn close_write(&mut self, error: Option<Self::Error>);
    fn abort(&mut self);
}

impl<W: WritableBody + ?Sized> WritableBody for &mut W {
    type Data = W::Data;
    type Error = W::Error;
    async fn write_frame(
        &mut self,
        frame: BodyFrame<Self::Data>,
        cancellation: &Cancellation,
    ) -> Result<(), Self::Error> {
        (**self).write_frame(frame, cancellation).await
    }
    async fn write(
        &mut self,
        data: Self::Data,
        cancellation: &Cancellation,
    ) -> Result<(), Self::Error> {
        (**self).write(data, cancellation).await
    }
    async fn flush(&mut self, cancellation: &Cancellation) -> Result<(), Self::Error> {
        (**self).flush(cancellation).await
    }
    fn close_write(&mut self, error: Option<Self::Error>) {
        (**self).close_write(error)
    }
    fn abort(&mut self) {
        (**self).abort()
    }
}
