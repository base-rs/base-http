// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use base_scheduler::HeapBufferPool;

#[test]
fn native_addresses_decode_unaligned_ipv4_and_ipv6_metadata_without_exposing_it_as_payload() {
    let addresses = [
        "127.0.0.2:43210".parse().unwrap(),
        SocketAddrV6::new("fe80::1234".parse().unwrap(), 12345, 0x12345678, 9).into(),
    ];
    for expected in addresses {
        let address = socket2::SockAddr::from(expected);
        let pool = HeapBufferPool::new(1, 256).unwrap();
        let mut storage = pool.try_acquire().unwrap();
        storage.as_mut().fill(0xA5);
        let name = 1..1 + address.len() as usize;
        // SAFETY: SockAddr::from initializes the native address, whose reported length remains valid through this copy.
        let native = unsafe {
            std::slice::from_raw_parts(address.as_ptr().cast::<u8>(), address.len() as usize)
        };
        storage.as_mut()[name.clone()].copy_from_slice(native);
        storage.as_mut()[200..204].copy_from_slice(b"data");
        let payload = crate::IoUringBuffer::new(storage, 200, 4);
        let mut message = IoUringRecvMessage::native(payload, name, true, true);
        let pointer = message.payload().as_ptr();
        assert_eq!(message.payload(), b"data");
        let view = message.address().unwrap();
        assert_eq!(view.to_socket_address(), expected);
        drop(view);
        message.payload_mut()[0] = b'X';
        assert_eq!(message.address().unwrap().to_socket_address(), expected);
        assert!(message.truncated());
        assert!(message.control_truncated());
        let payload = message.into_payload();
        assert_eq!(payload.as_ref(), b"Xata");
        assert_eq!(payload.as_ref().as_ptr(), pointer);
        let frozen = base_scheduler::ReceiveBuffer::freeze(payload);
        drop(pool);
        assert_eq!(frozen.as_ref(), b"Xata");
    }
}

#[test]
fn incomplete_empty_and_unsupported_native_addresses_have_no_ip_view() {
    for bytes in [&[][..], &[0], &[0, 0]] {
        assert!(AddressView::native(bytes).is_none());
    }
    for family in [libc::AF_INET, libc::AF_INET6] {
        assert!(AddressView::native(&(family as libc::sa_family_t).to_ne_bytes()).is_none());
    }
}

#[test]
fn portable_metadata_and_empty_payloads_use_the_same_message_contract() {
    for address in [None, Some("127.0.0.1:1234".parse().unwrap())] {
        let pool = HeapBufferPool::new(1, 8).unwrap();
        let buffer = pool.try_acquire().unwrap();
        let payload = crate::IoUringBuffer::new(buffer, 0, 0);
        let message = IoUringRecvMessage::portable(OwnedSocketRecvMessage::new(
            base_scheduler::SocketRecvResult {
                bytes: 0,
                address,
                truncated: false,
                control_truncated: false,
            },
            payload,
        ));
        assert_eq!(
            message.address().map(|view| view.to_socket_address()),
            address
        );
        assert!(!message.truncated());
        assert!(!message.control_truncated());
        assert!(message.payload().is_empty());
        assert!(message.into_payload().as_ref().is_empty());
    }
}
