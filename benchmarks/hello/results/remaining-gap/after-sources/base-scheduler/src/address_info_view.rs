// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{AddressInfo, SocketAddress, SocketProtocol, SocketType};

/// Portable accessors for a resolved IP endpoint and its socket creation parameters. A view may borrow a native resolver entry; accessing one field need not copy or normalize the others.
pub trait AddressInfoView {
    /// Decode the IP endpoint on demand, preserving IPv6 flow information and scope identifiers.
    fn address(&self) -> SocketAddress;

    fn socket_type(&self) -> SocketType;

    /// `None` represents the resolver's zero/default protocol.
    fn protocol(&self) -> Option<SocketProtocol>;

    /// Explicitly materialize a small, owned portable record. This conversion requires no heap allocation.
    fn to_address_info(&self) -> AddressInfo {
        AddressInfo {
            address: self.address(),
            socket_type: self.socket_type(),
            protocol: self.protocol(),
        }
    }
}

impl<T: AddressInfoView + ?Sized> AddressInfoView for &T {
    fn address(&self) -> SocketAddress {
        T::address(self)
    }

    fn socket_type(&self) -> SocketType {
        T::socket_type(self)
    }

    fn protocol(&self) -> Option<SocketProtocol> {
        T::protocol(self)
    }
}
