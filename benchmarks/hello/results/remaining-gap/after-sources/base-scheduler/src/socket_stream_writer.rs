// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, IoBuf, OperationError, OperationResult};
use std::{
    future::Future,
    io::{self, IoSlice},
};

/// One ordered output lane on a stream socket, bound to its creating scheduler. Independent readers may operate concurrently. Separate writers or direct socket aliases do not share this handle's ordering.
///
/// If an owned-operation future is abandoned while backend access continues, subsequent operations may report `WouldBlock` until it retires. Await `close` to establish retirement; separate handles provide independent lanes.
pub trait SocketStreamWriter: Send {
    type Buffer: IoBuf;

    /// Await previous accepted output, then write once from borrowed storage. The count can be short; this call does not accept the unwritten suffix. No backend accesses the borrowed bytes between polls.
    fn write(
        &mut self,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;

    /// Write once from borrowed vectors after previous accepted output completes. Short progress does not advance the caller's vectors.
    fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;

    /// Accept a complete owned logical write, waiting for its predecessor before acceptance. Success transfers the owner and lets output progress without further polling. Rejection returns the unchanged owner and writes none of it. Cancellation governs accepted output until its send result; an accepted error or cancellation ends this output sequence because partial bytes cannot be rolled back. Owners remain retained until backend access retires, including after drop or forget.
    fn submit(
        &mut self,
        buffer: Self::Buffer,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<(), (OperationError, Self::Buffer)>> + Send;

    /// Wait for accepted output to complete, including short-write retries. Success means socket acceptance, not peer acknowledgement or memory-retirement notification. Cancelling this wait does not cancel already accepted output.
    fn flush(
        &mut self,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;

    /// Perform one borrowed transfer with explicit portable flags. Payload access ends before each poll returns; successful progress is returned in that poll.
    fn write_with_flags(
        &mut self,
        buffer: &[u8],
        flags: crate::SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send {
        async move {
            self.write_vectored_with_flags(&[std::io::IoSlice::new(buffer)], flags, cancellation)
                .await
        }
    }

    /// Perform one borrowed vectored transfer with explicit flags. Vectors are never advanced implicitly; a short result leaves their ranges unchanged.
    fn write_vectored_with_flags(
        &mut self,
        buffers: &[std::io::IoSlice<'_>],
        flags: crate::SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;

    /// Perform one transfer while retaining a stable owner and return that original owner with every collected result. Backends may retain it independently until access retires after waiter abandonment.
    fn write_owned<B: crate::IoBuf>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = crate::BufferResult<B>> + Send {
        async move {
            let result = self.write(buffer.as_ref(), cancellation).await;
            (result, buffer)
        }
    }

    /// Transfer one message using an owned buffer sequence with explicit flags. Return the original sequence and its unchanged ranges on success, error, and cancellation. Kernel or worker access retains the full owner and stable I/O vectors through retirement, including after future abandonment. This awaits earlier accepted output before transferring bytes.
    fn write_owned_vectored<Buffers: crate::IoBufSequence>(
        &mut self,
        buffers: Buffers,
        flags: crate::SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = (OperationResult<usize>, Buffers)> + Send;

    /// Stop admission, cancel unfinished output, and await backend access and registration retirement. Flush first when successful delivery is required. This does not shut down or close the socket. Drop requests retirement without blocking; retry an abandoned close to establish completion.
    fn close(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}
