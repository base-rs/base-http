// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{
    IoBuf, IoBufMut, OwnedSocketRecvMessage, SocketAddress, SocketAddressView, SocketRecvMessage,
    SocketRecvMessageMut,
};
use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6},
    ops::Range,
};

/// A completed receive with payload ownership and backend-native or portable metadata. Native source addresses remain in the retained receive allocation until requested.
pub struct IoUringRecvMessage<B: IoBuf = crate::IoUringBuffer> {
    payload: B,
    address: Address,
    truncated: bool,
    control_truncated: bool,
}

enum Address {
    Portable(Option<SocketAddress>),
    Native(Range<usize>),
}

/// Access to initialized metadata outside a completed payload. Only the backend's retained buffer implementations provide this internal view.
pub(crate) trait MessageStorage: IoBuf {
    fn storage(&self) -> &[u8];
}

impl<B: IoBuf> IoUringRecvMessage<B> {
    pub(crate) fn native(
        payload: B,
        name: Range<usize>,
        truncated: bool,
        control_truncated: bool,
    ) -> Self {
        Self {
            payload,
            address: Address::Native(name),
            truncated,
            control_truncated,
        }
    }

    pub(crate) fn portable(message: OwnedSocketRecvMessage<B>) -> Self {
        Self {
            address: Address::Portable(
                message.address().map(|address| address.to_socket_address()),
            ),
            truncated: message.truncated(),
            control_truncated: message.control_truncated(),
            payload: message.into_payload(),
        }
    }
}

impl<B: MessageStorage> SocketRecvMessage for IoUringRecvMessage<B> {
    type Payload = B;

    fn address(&self) -> Option<impl SocketAddressView + '_> {
        match &self.address {
            Address::Portable(address) => address.as_ref().map(AddressView::Portable),
            Address::Native(range) => AddressView::native(&self.payload.storage()[range.clone()]),
        }
    }

    fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }
    fn truncated(&self) -> bool {
        self.truncated
    }
    fn control_truncated(&self) -> bool {
        self.control_truncated
    }
    fn into_payload(self) -> B {
        self.payload
    }
}

impl<B: MessageStorage + IoBufMut> SocketRecvMessageMut for IoUringRecvMessage<B> {
    fn payload_mut(&mut self) -> &mut [u8] {
        self.payload.as_mut()
    }
}

enum AddressView<'a> {
    Portable(&'a SocketAddress),
    Ipv4(&'a [u8]),
    Ipv6(&'a [u8]),
}

impl<'a> AddressView<'a> {
    fn native(bytes: &'a [u8]) -> Option<Self> {
        let family = libc::sa_family_t::from_ne_bytes(bytes.get(..2)?.try_into().unwrap());
        match family as i32 {
            libc::AF_INET => bytes
                .get(..std::mem::size_of::<libc::sockaddr_in>())
                .map(Self::Ipv4),
            libc::AF_INET6 => bytes
                .get(..std::mem::size_of::<libc::sockaddr_in6>())
                .map(Self::Ipv6),
            _ => None,
        }
    }
}

impl SocketAddressView for AddressView<'_> {
    fn to_socket_address(&self) -> SocketAddress {
        match self {
            Self::Portable(address) => **address,
            Self::Ipv4(bytes) => {
                // SAFETY: Construction checked the family and complete initialized sockaddr_in length. The kernel metadata need not be aligned for a Rust reference.
                let address =
                    unsafe { bytes.as_ptr().cast::<libc::sockaddr_in>().read_unaligned() };
                SocketAddrV4::new(
                    Ipv4Addr::from(address.sin_addr.s_addr.to_ne_bytes()),
                    u16::from_be(address.sin_port),
                )
                .into()
            }
            Self::Ipv6(bytes) => {
                // SAFETY: Construction checked the family and complete initialized sockaddr_in6 length. Reading a value permits unaligned native metadata.
                let address =
                    unsafe { bytes.as_ptr().cast::<libc::sockaddr_in6>().read_unaligned() };
                SocketAddrV6::new(
                    Ipv6Addr::from(address.sin6_addr.s6_addr),
                    u16::from_be(address.sin6_port),
                    address.sin6_flowinfo,
                    address.sin6_scope_id,
                )
                .into()
            }
        }
    }
}

#[cfg(test)]
mod tests;
