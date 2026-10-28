// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{
    AddressHints, AddressInfo, AddressInfoCollection, AddressInfoView, AddressResolution,
    AddressService, Cancellation, OperationError, OperationResult, SocketProtocol, SocketType,
};
use std::future::Future;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Cached(Arc<[AddressInfo]>);

impl AddressInfoCollection for Cached {
    fn entries(&self) -> impl Iterator<Item: AddressInfoView + Send> + Send {
        self.0.iter()
    }
}

// A one-query fixture proving that a resolver need not clone its result owner.
struct Resolver<R>(Mutex<Option<R>>);

impl<R: AddressInfoCollection> AddressResolution for Resolver<R> {
    type Addresses = R;

    async fn address_resolve(
        &self,
        _: Option<&str>,
        _: AddressService<'_>,
        _: AddressHints,
        cancellation: &Cancellation,
    ) -> OperationResult<R> {
        cancellation.check()?;
        Ok(self
            .0
            .lock()
            .unwrap()
            .take()
            .expect("fixture resolves once"))
    }
}

async fn query<S: AddressResolution>(
    scheduler: &S,
    cancellation: &Cancellation,
) -> OperationResult<S::Addresses> {
    scheduler
        .address_resolve(
            Some("example.test"),
            AddressService::Port(80),
            AddressHints::default(),
            cancellation,
        )
        .await
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("the table fixture is immediately ready"),
    }
}

fn inspect<R: AddressInfoCollection>(result: R, expected: &[AddressInfo]) {
    let resolver = Resolver(Mutex::new(Some(result)));
    let live = Cancellation::new();
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let unpolled = query(&resolver, &live);
    drop(unpolled);
    assert!(resolver.0.lock().unwrap().is_some());
    assert!(matches!(
        ready(query(&resolver, &cancelled)),
        Err(OperationError::Cancelled(_))
    ));
    assert!(resolver.0.lock().unwrap().is_some());

    let allocations = allocation_counter::measure(|| {
        let result = ready(query(&resolver, &live)).unwrap();
        assert_eq!(result.entries().count(), expected.len());
        for _ in 0..16 {
            for (entry, expected) in result.entries().zip(expected) {
                assert_eq!(entry.address(), expected.address);
                assert_eq!(entry.socket_type(), expected.socket_type);
                assert_eq!(entry.protocol(), expected.protocol);
                assert_eq!(entry.to_address_info(), *expected);
            }
        }
    });
    assert_eq!(allocations.count_total, 0);
}

#[test]
fn portable_arrays_vectors_boxed_slices_and_cached_results_preserve_entries_without_allocating() {
    let entries = [
        AddressInfo {
            address: "127.0.0.1:80".parse().unwrap(),
            socket_type: SocketType::Stream,
            protocol: Some(SocketProtocol::Tcp),
        },
        AddressInfo {
            address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1234, 42, 7).into(),
            socket_type: SocketType::Other(123),
            protocol: Some(SocketProtocol::Other(456)),
        },
        AddressInfo {
            address: "127.0.0.1:80".parse().unwrap(),
            socket_type: SocketType::Datagram,
            protocol: None,
        },
        AddressInfo {
            address: "127.0.0.1:80".parse().unwrap(),
            socket_type: SocketType::Stream,
            protocol: Some(SocketProtocol::Tcp),
        },
    ];
    inspect(entries, &entries);
    inspect(entries.to_vec(), &entries);
    inspect(Box::new(entries) as Box<[AddressInfo]>, &entries);
    inspect(Cached(Arc::from(entries)), &entries);
    inspect([] as [AddressInfo; 0], &[]);
}

#[test]
fn borrowed_entry_iteration_can_cross_polling_threads() {
    let entries = [AddressInfo {
        address: "127.0.0.1:80".parse().unwrap(),
        socket_type: SocketType::Stream,
        protocol: None,
    }];
    std::thread::scope(|scope| {
        let mut iterator = entries.entries();
        let entry = iterator.next().unwrap();
        scope
            .spawn(move || assert_eq!(entry.address(), entries[0].address))
            .join()
            .unwrap();
        scope
            .spawn(move || assert!(iterator.next().is_none()))
            .join()
            .unwrap();
    });
}
