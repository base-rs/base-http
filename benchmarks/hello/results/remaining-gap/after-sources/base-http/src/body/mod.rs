// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.
// Copyright, 2023, by Bruno Sutic.

//! Native HTTP bodies, following `Protocol::HTTP::Body`.
//!
//! [`ReadableBody`] combines synchronous observations and abandonment with
//! asynchronous chunk reads using explicit cancellation. I/O-backed
//! implementations retain explicitly constructed scheduler-bound capabilities.
//!
//! Body length describes the whole representation. It does not decrease as chunks
//! are consumed, and a [`HeadBody`] body may report a nonzero length while emitting no
//! data. An empty chunk is data; only `None` from a read is EOF.
//!
//! [`BodyReader`] and [`BodyWriter`] are bounded consumer/producer halves.
//! A protocol driver pulls data directly; bodies do not spawn tasks.

use base_scheduler::Cancellation;
use std::future::Future;

mod buffered;
mod empty;
mod error;
mod head;
mod writable;

pub use buffered::BufferedBody;
pub use empty::EmptyBody;
pub use error::BodyError;
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

    /// Whether no unread data remains. False can mean unknown; an open queue
    /// containing no chunks is not exhausted. This does not suppress read errors.
    fn is_empty(&self) -> bool {
        false
    }

    /// Total representation length, independent of the current read position.
    fn length(&self) -> Option<u64> {
        None
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

    /// Move trailing fields into the message after EOF. Called by message read
    /// helpers and protocol drivers; repeated calls must be harmless.
    /// Append through `Fields::mark_trailers`, preserving the initial section.
    fn finish(&mut self, _fields: &mut crate::Fields) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Read one chunk. Pre-cancellation prevents consumption; `None` is EOF.
    /// Dropping a pending read is not transport retirement.
    fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Option<Self::Data>, Self::Error>> + Send;
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
    fn is_rewindable(&self) -> bool {
        (**self).is_rewindable()
    }
    fn rewind(&mut self) -> bool {
        (**self).rewind()
    }
    fn close(&mut self, error: Option<Self::Error>) {
        (**self).close(error)
    }

    fn finish(&mut self, fields: &mut crate::Fields) -> Result<(), Self::Error> {
        (**self).finish(fields)
    }

    async fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Option<Self::Data>, Self::Error> {
        (**self).read(cancellation).await
    }
}

/// A body producer accepting whole chunks with backpressure.
/// Normal closure preserves accepted data; failure abandons unfinished output.
/// Flushing exposes accepted chunks to the consumer, without promising peer receipt.
pub trait WritableBody: Send {
    type Data: AsRef<[u8]> + Send;
    type Error: std::error::Error + Send + Sync + 'static;

    fn write(
        &mut self,
        data: Self::Data,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
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
