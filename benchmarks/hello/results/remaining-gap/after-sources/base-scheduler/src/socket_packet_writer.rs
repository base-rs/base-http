// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferResult, Cancellation, IoBuf, OperationResult, SocketAddress};
use std::{
    future::Future,
    io::{self, IoSlice},
};

/// Sequential packet sends through a fixed execution service. Every write is one send attempt, including empty packets; it never splits a packet into separate sends. `None` uses the connected peer.
///
/// If an owned-operation future is abandoned while backend access continues, subsequent operations may report `WouldBlock` until it retires. Await `close` to establish retirement; separate handles provide independent lanes.
pub trait SocketPacketWriter: Send {
    fn write(
        &mut self,
        buffer: &[u8],
        address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;

    /// Send the vectors as one packet, preserving their order without advancing them. The returned count is the backend's single-transfer result.
    fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;

    /// Retain the owner while sending one packet and return it on success, error, or cancellation. Abandonment drops the owner only after all borrowed access has ended.
    fn write_owned<B: IoBuf>(
        &mut self,
        buffer: B,
        address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    /// Perform one borrowed transfer with explicit portable flags. Payload access ends before each poll returns; successful progress is returned in that poll.
    fn write_with_flags(
        &mut self,
        buffer: &[u8],
        address: Option<crate::SocketAddress>,
        flags: crate::SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send {
        async move {
            self.write_vectored_with_flags(
                &[std::io::IoSlice::new(buffer)],
                address,
                flags,
                cancellation,
            )
            .await
        }
    }

    /// Perform one borrowed vectored transfer with explicit flags. Vectors are never advanced implicitly; a short result leaves their ranges unchanged.
    fn write_vectored_with_flags(
        &mut self,
        buffers: &[std::io::IoSlice<'_>],
        address: Option<crate::SocketAddress>,
        flags: crate::SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;

    /// Transfer one message using an owned buffer sequence with explicit flags. Return the original sequence and its unchanged ranges on success, error, and cancellation. Kernel or worker access retains the full owner and stable I/O vectors through retirement, including after future abandonment.
    fn write_owned_vectored<Buffers: crate::IoBufSequence>(
        &mut self,
        buffers: Buffers,
        address: Option<crate::SocketAddress>,
        flags: crate::SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = (OperationResult<usize>, Buffers)> + Send;

    /// Stop writing and await registration retirement without cancellation. Stop using the writer when close begins; retry close after errors or abandonment to establish retirement.
    fn close(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}
