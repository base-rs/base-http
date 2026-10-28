// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, OperationResult, ReceiveBuffer};
use std::{
    future::Future,
    io::{self, IoSliceMut},
};

/// One sequential input lane on a stream socket, bound to its creating scheduler. Independent reader and writer handles may operate concurrently. Backend registration and worker migration are internal details; moving a handle never changes its scheduler.
///
/// Owned chunks remain valid independently of the reader and can be retained for parsing or transferred to another task. Borrowed operations access payloads only synchronously within each poll, so dropping or forgetting a pending future cannot leave backend access to caller storage. Ordinary errors and cancellation end the current wait, without closing the reader or rolling back consumed bytes. No framing or read-ahead is required.
///
/// If an owned-operation future is abandoned while backend access continues, subsequent operations may report `WouldBlock` until it retires. Await `close` to establish retirement; separate handles provide independent lanes.
pub trait SocketStreamReader: Send {
    type Buffer: ReceiveBuffer;

    /// Read an owned initialized chunk. `None` means EOF or a closed reader. Holding every pool lease waits for capacity with cancellation. Idle readiness waits need not retain a payload buffer.
    fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Option<Self::Buffer>>> + Send;

    /// Read once into borrowed storage, returning a possibly short count. Zero with nonempty storage is EOF. An empty destination completes with zero after checking that the handle is open and cancellation has not been requested, without establishing EOF. Successful progress wins concurrent cancellation.
    fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;

    /// Scatter a single read across borrowed ranges without allocating an I/O-vector array or copying payloads. Entirely empty storage follows `read_into`'s zero-capacity behavior.
    fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;

    /// Perform one borrowed transfer with explicit portable flags. Payload access ends before each poll returns; successful progress is returned in that poll.
    fn read_into_with_flags(
        &mut self,
        buffer: &mut [u8],
        flags: crate::SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send {
        async move {
            self.read_vectored_with_flags(
                &mut [std::io::IoSliceMut::new(buffer)],
                flags,
                cancellation,
            )
            .await
            .map(|result| result.bytes)
        }
    }

    /// Perform one borrowed vectored transfer with explicit flags. Vectors are never advanced implicitly; a short result leaves their ranges unchanged.
    fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [std::io::IoSliceMut<'_>],
        flags: crate::SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<crate::SocketRecvResult>> + Send;

    /// Perform one transfer while retaining a stable owner and return that original owner with every collected result. Backends may retain it independently until access retires after waiter abandonment.
    fn read_owned<B: crate::IoBufMut>(
        &mut self,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = crate::BufferResult<B>> + Send {
        async move {
            let result = self.read_into(buffer.as_mut(), cancellation).await;
            (result, buffer)
        }
    }

    /// Transfer one message using an owned buffer sequence with explicit flags. Return the original sequence and its unchanged ranges on success, error, and cancellation. Kernel or worker access retains the full owner and stable I/O vectors through retirement, including after future abandonment.
    fn read_owned_vectored<Buffers: crate::IoBufSequenceMut>(
        &mut self,
        buffers: Buffers,
        flags: crate::SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = (OperationResult<crate::SocketRecvResult>, Buffers)> + Send;

    /// Stop input and await backend access and registration retirement. This does not close or shut down the socket. Cleanup remains available after cancellation; retry it if its future is dropped. Returned chunks remain valid. Drop the retired reader before exclusively closing its socket.
    fn close(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}
