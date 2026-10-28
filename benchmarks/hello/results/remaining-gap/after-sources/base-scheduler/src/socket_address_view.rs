// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::SocketAddress;

/// An IP endpoint exposed by an address representation chosen by its provider. A view can borrow native metadata or refer to an already materialized portable address. The contract specifies the endpoint, without prescribing storage, copying, or allocation costs.
pub trait SocketAddressView: Send + Sync {
    /// Materialize the portable endpoint, preserving IPv6 flow information and scope identifiers.
    fn to_socket_address(&self) -> SocketAddress;
}

impl SocketAddressView for SocketAddress {
    fn to_socket_address(&self) -> SocketAddress {
        *self
    }
}

impl<T: SocketAddressView + ?Sized> SocketAddressView for &T {
    fn to_socket_address(&self) -> SocketAddress {
        T::to_socket_address(self)
    }
}
