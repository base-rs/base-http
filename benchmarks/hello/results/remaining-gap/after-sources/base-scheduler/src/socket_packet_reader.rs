// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, OperationResult, ReceiveBuffer, SocketRecvMessageMut, SocketRecvResult};
use std::{
    future::Future,
    io::{self, IoSliceMut},
};

/// Sequential packet reception through a fixed execution service. Each read consumes at most one packet and preserves its sender and truncation metadata. Empty packets are messages, not EOF.
///
/// If an owned-operation future is abandoned while backend access continues, subsequent operations may report `WouldBlock` until it retires. Await `close` to establish retirement; separate handles provide independent lanes.
pub trait SocketPacketReader: Send {
    type Message: SocketRecvMessageMut<Payload: ReceiveBuffer>;

    /// Receive independently owned storage. `None` means the reader has been closed. Retained messages remain valid after subsequent reads, pool replacement, and close. Pool exhaustion waits with cancellation.
    fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Option<Self::Message>>> + Send;

    /// Receive one packet into reused borrowed storage. A short buffer truncates that packet; no subsequent read recovers its discarded suffix. An empty packet returns zero bytes successfully.
    fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<SocketRecvResult>> + Send;

    /// Receive one packet across borrowed vectors in order. The vectors are not advanced; the returned count and truncation flags describe the whole packet attempt.
    fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<SocketRecvResult>> + Send;

    /// Perform one borrowed transfer with explicit portable flags. Payload access ends before each poll returns; successful progress is returned in that poll.
    fn read_into_with_flags(
        &mut self,
        buffer: &mut [u8],
        flags: crate::SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<crate::SocketRecvResult>> + Send {
        async move {
            self.read_vectored_with_flags(
                &mut [std::io::IoSliceMut::new(buffer)],
                flags,
                cancellation,
            )
            .await
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
    ) -> impl Future<Output = (OperationResult<crate::SocketRecvResult>, B)> + Send {
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

    /// Stop reading and await registration retirement without cancellation. Stop using the reader when close begins, including after errors or an abandoned close wait. Close may be retried; completed messages stay valid.
    fn close(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}
