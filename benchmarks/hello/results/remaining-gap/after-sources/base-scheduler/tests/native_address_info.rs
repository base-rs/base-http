// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(unix)]

use base_scheduler::{
    AddressInfoCollection, AddressInfoView, SocketAddress, SocketProtocol, SocketType,
};
use std::future::{Future, pending};
use std::net::{Ipv6Addr, SocketAddrV6};
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Waker};

// Test-only native IPv6 fixture. Production resolver policy and syscalls remain backend responsibilities.
struct NativeResult {
    head: NonNull<libc::addrinfo>,
    freed: Arc<AtomicUsize>,
}

// SAFETY: This owner exclusively retains the native list. Its records and all pointed-to data are immutable until the matching freeaddrinfo call on drop.
unsafe impl Send for NativeResult {}
unsafe impl Sync for NativeResult {}

#[repr(transparent)]
struct NativeEntry(libc::addrinfo);

// SAFETY: Views are only borrowed from the immutable, validated list owned by NativeResult; they cannot outlive or mutate it.
unsafe impl Send for NativeEntry {}
unsafe impl Sync for NativeEntry {}

struct NativeEntries<'a>(Option<&'a NativeEntry>);

impl<'a> Iterator for NativeEntries<'a> {
    type Item = &'a NativeEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.0.take()?;
        // SAFETY: Every next pointer belongs to the same retained native list. The transparent wrapper has the addrinfo layout, and the borrow is bounded by its owner.
        self.0 = unsafe { entry.0.ai_next.cast::<NativeEntry>().as_ref() };
        Some(entry)
    }
}

impl AddressInfoCollection for NativeResult {
    fn entries(&self) -> impl Iterator<Item: AddressInfoView + Send> + Send {
        // SAFETY: The original head is retained until self is dropped, and the transparent entry borrow cannot escape this owner's lifetime.
        NativeEntries(Some(unsafe { &*self.head.as_ptr().cast::<NativeEntry>() }))
    }
}

impl AddressInfoView for NativeEntry {
    fn address(&self) -> SocketAddress {
        // SAFETY: The fixture validates family, length, and address alignment before exposing entries; the list retains this sockaddr_in6.
        let address = unsafe { &*self.0.ai_addr.cast::<libc::sockaddr_in6>() };
        SocketAddrV6::new(
            Ipv6Addr::from(address.sin6_addr.s6_addr),
            u16::from_be(address.sin6_port),
            address.sin6_flowinfo,
            address.sin6_scope_id,
        )
        .into()
    }

    fn socket_type(&self) -> SocketType {
        match self.0.ai_socktype {
            libc::SOCK_STREAM => SocketType::Stream,
            other => SocketType::Other(other),
        }
    }

    fn protocol(&self) -> Option<SocketProtocol> {
        match self.0.ai_protocol {
            0 => None,
            libc::IPPROTO_TCP => Some(SocketProtocol::Tcp),
            other => Some(SocketProtocol::Other(other)),
        }
    }
}

impl Drop for NativeResult {
    fn drop(&mut self) {
        // SAFETY: This unchanged head came from getaddrinfo and has exactly one owner. Entry borrows have ended before the owner can be dropped.
        unsafe { libc::freeaddrinfo(self.head.as_ptr()) };
        self.freed.fetch_add(1, Ordering::SeqCst);
    }
}

fn lookup() -> *mut libc::addrinfo {
    // SAFETY: addrinfo's output-only fields may be null in hints; populated hint fields are numeric constants.
    let mut hints: libc::addrinfo = unsafe { std::mem::zeroed() };
    hints.ai_family = libc::AF_INET6;
    hints.ai_socktype = libc::SOCK_STREAM;
    hints.ai_protocol = libc::IPPROTO_TCP;
    hints.ai_flags = libc::AI_NUMERICHOST | libc::AI_NUMERICSERV;
    let mut head = ptr::null_mut();
    // SAFETY: The terminated numeric strings and hints live through the call, and head is valid writable output storage. No network lookup is required.
    assert_eq!(
        unsafe { libc::getaddrinfo(c"fe80::1%7".as_ptr(), c"1234".as_ptr(), &hints, &mut head) },
        0
    );
    head
}

fn wrap(head: *mut libc::addrinfo, freed: Arc<AtomicUsize>) -> NativeResult {
    let result = NativeResult {
        head: NonNull::new(head).unwrap(),
        freed,
    };
    let mut current = head;
    while !current.is_null() {
        // SAFETY: This is a successful getaddrinfo list already retained by result.
        let entry = unsafe { &*current };
        assert_eq!(entry.ai_family, libc::AF_INET6);
        assert!(!entry.ai_addr.is_null());
        assert!(entry.ai_addrlen as usize >= std::mem::size_of::<libc::sockaddr_in6>());
        assert_eq!(
            entry.ai_addr as usize % std::mem::align_of::<libc::sockaddr_in6>(),
            0
        );
        current = entry.ai_next;
    }
    result
}

#[test]
fn native_list_wrapping_validation_and_borrowed_iteration_need_no_extra_allocation() {
    let head = lookup();
    let freed = Arc::new(AtomicUsize::new(0));
    let mut owner = None;
    let expected: SocketAddress = SocketAddrV6::new("fe80::1".parse().unwrap(), 1234, 0, 7).into();
    let allocations = allocation_counter::measure(|| {
        let result = wrap(head, Arc::clone(&freed));
        assert_eq!(result.head.as_ptr(), head);
        for entry in result.entries() {
            assert_eq!(entry.address(), expected);
            assert_eq!(entry.socket_type(), SocketType::Stream);
            assert_eq!(entry.protocol(), Some(SocketProtocol::Tcp));
            assert_eq!(entry.to_address_info().address, expected);
        }
        // Ending iteration early does not free the list or copy an entry owner.
        let _ = result.entries().next().unwrap();
        assert_eq!(freed.load(Ordering::SeqCst), 0);
        owner = Some(result);
    });
    assert_eq!(allocations.count_total, 0);
    drop(owner);
    assert_eq!(freed.load(Ordering::SeqCst), 1);
}

#[test]
fn abandoned_work_releases_its_native_result_exactly_once() {
    let freed = Arc::new(AtomicUsize::new(0));
    let result = wrap(lookup(), Arc::clone(&freed));
    let mut operation = Box::pin(async move {
        pending::<()>().await;
        result
    });
    assert!(
        operation
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(operation);
    assert_eq!(freed.load(Ordering::SeqCst), 1);

    let result = wrap(lookup(), Arc::clone(&freed));
    std::thread::scope(|scope| {
        let mut entries = result.entries();
        scope
            .spawn(move || assert!(entries.next().is_some()))
            .join()
            .unwrap();
    });
    drop(result);
    assert_eq!(freed.load(Ordering::SeqCst), 2);
}
