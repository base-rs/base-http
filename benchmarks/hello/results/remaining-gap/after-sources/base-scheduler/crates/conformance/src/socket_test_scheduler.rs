// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{
    SocketOperation, SocketPacketReading, SocketPacketWriting, SocketStreamReading,
    SocketStreamWriting,
};

/// Complete socket I/O profile used by the loopback conformance fixtures.
pub trait SocketTestScheduler:
    SocketStreamReading
    + SocketStreamWriting
    + SocketPacketReading
    + SocketPacketWriting
    + SocketOperation<Socket: 'static>
{
}
impl<S> SocketTestScheduler for S where
    S: SocketStreamReading
        + SocketStreamWriting
        + SocketPacketReading
        + SocketPacketWriting
        + SocketOperation<Socket: 'static>
{
}
