// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{IoBuf, SocketAddressView};

/// An owned received stream chunk or datagram, with backend-associated payload storage and metadata. The message may move between workers and outlive its receiver and scheduler. Its accessors describe contents and ownership, without prescribing allocation, copying, registration, or receive strategy.
///
/// Address views borrow the message and cannot survive extraction of its payload:
///
/// ```compile_fail
/// use base_scheduler::{OwnedSocketRecvMessage, SocketAddressView, SocketRecvMessage, SocketRecvResult};
/// let message = OwnedSocketRecvMessage::new(SocketRecvResult {
///     bytes: 1, address: Some("127.0.0.1:1234".parse().unwrap()),
///     truncated: false, control_truncated: false,
/// }, vec![0]);
/// let address = message.address().unwrap();
/// let payload = message.into_payload();
/// let endpoint = address.to_socket_address();
/// ```
pub trait SocketRecvMessage: Send + 'static {
    type Payload: IoBuf;

    /// The source endpoint when supplied by the receive operation. Stream chunks may have no address; use `socket_peer_address` for the connected peer. The view's lifetime is bounded by this message. Its representation and any conversion costs are implementation details.
    fn address(&self) -> Option<impl SocketAddressView + '_>;

    /// Exactly the completed payload. Empty datagrams expose an empty slice. Headers, source-address storage, ancillary metadata, and unused capacity are outside this range.
    fn payload(&self) -> &[u8];

    /// Whether a datagram exceeded the supplied payload capacity. The exposed bytes are the received prefix, not the original datagram length.
    fn truncated(&self) -> bool;

    /// Whether the receive discarded ancillary data. This interface does not expose a control buffer.
    fn control_truncated(&self) -> bool;

    /// Consume the message and return independently owned storage exposing the same payload bytes and range as `payload()`, including any modifications through `SocketRecvMessageMut`. The owner can be passed to owned I/O or retained after receiver shutdown. Address views cannot outlive this transfer.
    fn into_payload(self) -> Self::Payload;
}
