// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::*;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn metadata(bytes: usize, address: Option<SocketAddress>) -> SocketRecvResult {
    SocketRecvResult {
        bytes,
        address,
        truncated: true,
        control_truncated: true,
    }
}

fn edit<M: SocketRecvMessageMut>(message: &mut M) {
    message.payload_mut()[0] = b'X';
}

#[test]
fn portable_messages_preserve_metadata_and_transfer_the_completed_payload() {
    for address in [
        None,
        Some("127.0.0.1:1234".parse().unwrap()),
        Some("[fe80::1%3]:4321".parse().unwrap()),
    ] {
        let mut message = OwnedSocketRecvMessage::new(metadata(4, address), b"data".to_vec());
        let pointer = message.payload().as_ptr();
        assert_eq!(
            message.address().map(|view| view.to_socket_address()),
            address
        );
        assert_eq!(message.payload(), b"data");
        edit(&mut message);
        assert_eq!(message.payload(), b"Xata");
        assert!(message.truncated());
        assert!(message.control_truncated());
        let payload = message.into_payload();
        assert_eq!(payload, b"Xata");
        assert_eq!(payload.as_ptr(), pointer);
    }
}

#[test]
fn empty_payloads_and_clear_flags_are_preserved() {
    let message = OwnedSocketRecvMessage::new(
        SocketRecvResult {
            bytes: 0,
            address: None,
            truncated: false,
            control_truncated: false,
        },
        Vec::new(),
    );
    assert!(message.payload().is_empty());
    assert!(message.address().is_none());
    assert!(!message.truncated());
    assert!(!message.control_truncated());
    assert!(message.into_payload().is_empty());
}

#[test]
#[should_panic(expected = "receive metadata and payload lengths differ")]
fn portable_message_rejects_an_inconsistent_payload_length() {
    OwnedSocketRecvMessage::new(metadata(2, None), vec![0]);
}

#[test]
fn pooled_payload_mutation_and_freezing_preserve_ownership_after_teardown() {
    let pool = HeapBufferPool::new(1, 8).unwrap();
    let mut buffer = pool.try_acquire().unwrap();
    buffer.as_mut()[..4].copy_from_slice(b"data");
    buffer.truncate(4);
    let mut message = OwnedSocketRecvMessage::new(metadata(4, None), buffer);
    edit(&mut message);
    let pointer = message.payload().as_ptr();
    let retained = message.into_payload().freeze();
    let view = retained.view(1..);
    drop((pool, retained));
    assert_eq!(view.as_ref(), b"ata");
    assert_eq!(view.as_ref().as_ptr(), pointer.wrapping_add(1));
}

struct ReadOnly(Vec<u8>);
impl AsRef<[u8]> for ReadOnly {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
// SAFETY: The immutable owner retains one initialized heap allocation with a stable range across moves.
unsafe impl IoBuf for ReadOnly {}

struct EncodedMessage {
    address: [u8; 6],
    payload: ReadOnly,
    decodes: Arc<AtomicUsize>,
}
struct EncodedAddress<'a> {
    bytes: &'a [u8; 6],
    decodes: &'a AtomicUsize,
}
impl SocketAddressView for EncodedAddress<'_> {
    fn to_socket_address(&self) -> SocketAddress {
        self.decodes.fetch_add(1, Ordering::Relaxed);
        SocketAddrV4::new(
            Ipv4Addr::new(self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3]),
            u16::from_be_bytes([self.bytes[4], self.bytes[5]]),
        )
        .into()
    }
}
impl SocketRecvMessage for EncodedMessage {
    type Payload = ReadOnly;
    fn address(&self) -> Option<impl SocketAddressView + '_> {
        Some(EncodedAddress {
            bytes: &self.address,
            decodes: &self.decodes,
        })
    }
    fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }
    fn truncated(&self) -> bool {
        false
    }
    fn control_truncated(&self) -> bool {
        false
    }
    fn into_payload(self) -> ReadOnly {
        self.payload
    }
}

#[test]
fn custom_immutable_messages_can_decode_metadata_lazily_and_move_between_workers() {
    let decodes = Arc::new(AtomicUsize::new(0));
    let message = EncodedMessage {
        address: [127, 0, 0, 1, 0x12, 0x34],
        payload: ReadOnly(b"custom".to_vec()),
        decodes: decodes.clone(),
    };
    let payload = std::thread::spawn(move || {
        assert_eq!(message.payload(), b"custom");
        let view = message.address().unwrap();
        assert_eq!(message.decodes.load(Ordering::Relaxed), 0);
        // The borrowed address view itself can be passed to another worker.
        std::thread::scope(|scope| {
            scope
                .spawn(|| assert_eq!(view.to_socket_address(), "127.0.0.1:4660".parse().unwrap()))
                .join()
                .unwrap()
        });
        drop(view);
        message.into_payload()
    })
    .join()
    .unwrap();
    assert_eq!(payload.as_ref(), b"custom");
    assert_eq!(decodes.load(Ordering::Relaxed), 1);
    let address: SocketAddress = "[fe80::2%4]:80".parse().unwrap();
    let reference = &address;
    assert_eq!(SocketAddressView::to_socket_address(&reference), address);
}
