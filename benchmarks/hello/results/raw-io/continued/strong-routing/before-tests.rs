// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{NativeSocket, ReadinessSelector, SelectorOptions};
use base_scheduler::*;
use futures_lite::future::{block_on, poll_once};
use std::{future::Future, pin::pin};

fn setup(workers: usize) -> (ReadinessSelector, Vec<ReadinessWorker>) {
    let (selector, drivers) =
        ReadinessSelector::with_workers(SelectorOptions::default(), workers).unwrap();
    (
        selector,
        drivers.into_iter().map(SelectorDriver::enter).collect(),
    )
}
fn drive<F: Future>(future: F, worker: &mut ReadinessWorker) -> F::Output {
    let mut future = pin!(future);
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(output) = block_on(poll_once(future.as_mut())) {
            return output;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "readiness operation stalled"
        );
        worker.turn(Some(Duration::from_millis(10))).unwrap();
    }
}
fn packet() -> NativeSocket {
    NativeSocket::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap().into()).unwrap()
}
fn read<'a>(
    selector: &'a ReadinessSelector,
    socket: &NativeSocket,
) -> impl Future<Output = (OperationResult<usize>, Vec<u8>)> + Send + 'a {
    let socket = socket.retain();
    async move {
        selector
            .socket_read(&socket, vec![0; 1], &Cancellation::never())
            .await
    }
}

#[test]
fn notification_during_syscall_survives_would_block_clear() {
    let direction = Direction::new();
    assert_eq!(direction.snapshot(), 0);
    direction.publish();
    let before = direction.snapshot();
    direction.publish();
    direction.clear(before);
    assert_eq!(direction.snapshot() & 1, 1);
    direction.clear(direction.snapshot());
    assert_eq!(direction.snapshot() & 1, 0);
}

#[test]
fn concurrent_registration_of_the_same_socket_reuses_one_live_record() {
    let (selector, _workers) = setup(2);
    let Some(group) = &selector.readiness else {
        return;
    };
    let socket = packet();
    let barrier = std::sync::Barrier::new(2);
    // Two operations can both observe no registration before either syscall
    // returns WouldBlock. Their subsequent registration must be idempotent.
    let (first, second) = std::thread::scope(|scope| {
        let register = || {
            barrier.wait();
            socket.registrations.get(group, socket.get_ref()).unwrap()
        };
        let first = scope.spawn(register);
        let second = scope.spawn(register);
        (first.join().unwrap(), second.join().unwrap())
    });
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(socket.registrations.entries.lock().unwrap().len(), 1);
    assert_eq!(
        group
            .controllers
            .iter()
            .map(|controller| { controller.table.lock().unwrap().registrations.len() })
            .sum::<usize>(),
        1
    );
    drop(socket);
    assert!(first.owner.table.lock().unwrap().registrations.is_empty());
}

#[test]
fn worker_delivers_to_all_waiters_and_abandonment_only_removes_its_listener() {
    let (selector, mut workers) = setup(1);
    let socket = packet();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut first = pin!(read(&selector, &socket));
    let mut abandoned = Box::pin(read(&selector, &socket));
    let mut second = pin!(read(&selector, &socket));
    assert!(block_on(poll_once(first.as_mut())).is_none());
    assert!(block_on(poll_once(abandoned.as_mut())).is_none());
    assert!(block_on(poll_once(second.as_mut())).is_none());
    // Dropping a distinct waiter cannot overwrite another waiter's waker.
    drop(abandoned);
    sender.send_to(b"a", socket.local_addr().unwrap()).unwrap();
    sender.send_to(b"b", socket.local_addr().unwrap()).unwrap();
    workers[0].turn(Some(Duration::from_secs(1))).unwrap();
    assert_eq!(drive(first, &mut workers[0]).0.unwrap(), 1);
    assert_eq!(drive(second, &mut workers[0]).0.unwrap(), 1);
    drop(workers);
    block_on(selector.shutdown());
}

#[test]
fn routing_uses_the_selected_group_and_restores_nested_workers() {
    let (first, first_drivers) =
        ReadinessSelector::with_workers(SelectorOptions::default(), 2).unwrap();
    let (second, second_drivers) =
        ReadinessSelector::with_workers(SelectorOptions::default(), 1).unwrap();
    let Some(first_group) = &first.readiness else {
        return;
    };
    let second_group = second.readiness.as_ref().unwrap();
    let mut drivers = first_drivers.into_iter();
    let outer = drivers.next().unwrap().enter();
    assert!(Arc::ptr_eq(
        &first_group.route(),
        &first_group.controllers[0]
    ));
    let inner = drivers.next().unwrap().enter();
    let unrelated = second_drivers.into_iter().next().unwrap().enter();
    assert!(Arc::ptr_eq(
        &first_group.route(),
        &first_group.controllers[1]
    ));
    assert!(Arc::ptr_eq(
        &second_group.route(),
        &second_group.controllers[0]
    ));
    drop(unrelated);
    // Dropping another group doesn't change this group's routing.
    assert!(Arc::ptr_eq(
        &first_group.route(),
        &first_group.controllers[1]
    ));
    drop(inner);
    assert!(Arc::ptr_eq(
        &first_group.route(),
        &first_group.controllers[0]
    ));
    drop(outer);
}

#[test]
fn a_pending_future_can_move_to_an_external_thread() {
    let (selector, mut workers) = setup(1);
    let socket = packet();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    std::thread::scope(|scope| {
        let mut future = Box::pin(read(&selector, &socket));
        assert!(block_on(poll_once(future.as_mut())).is_none());
        let (tx, rx) = std::sync::mpsc::channel();
        let waiting = scope.spawn(move || tx.send(block_on(future)).unwrap());
        sender.send_to(b"a", socket.local_addr().unwrap()).unwrap();
        workers[0].turn(Some(Duration::from_secs(1))).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let output = loop {
            if let Ok(output) = rx.try_recv() {
                break output;
            }
            if std::time::Instant::now() >= deadline {
                selector.close();
                panic!("migrated operation stalled");
            }
            workers[0].turn(Some(Duration::from_millis(10))).unwrap();
        };
        assert_eq!(output.0.unwrap(), 1);
        waiting.join().unwrap();
    });
}

#[test]
fn socket_can_be_used_by_different_selectors_and_exclusively_closed_remotely() {
    let (first, mut first_workers) = setup(1);
    let (second, mut second_workers) = setup(1);
    let socket = packet();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    {
        let mut one = pin!(read(&first, &socket));
        let mut two = pin!(read(&second, &socket));
        assert!(block_on(poll_once(one.as_mut())).is_none());
        assert!(block_on(poll_once(two.as_mut())).is_none());
        sender.send_to(b"a", socket.local_addr().unwrap()).unwrap();
        sender.send_to(b"b", socket.local_addr().unwrap()).unwrap();
        first_workers[0].turn(Some(Duration::from_secs(1))).unwrap();
        second_workers[0]
            .turn(Some(Duration::from_secs(1)))
            .unwrap();
        assert_eq!(drive(one, &mut first_workers[0]).0.unwrap(), 1);
        assert_eq!(drive(two, &mut second_workers[0]).0.unwrap(), 1);
    }
    let group = first.readiness.clone();
    std::thread::scope(|scope| {
        scope
            .spawn(|| block_on(first.socket_close(socket)).unwrap())
            .join()
            .unwrap();
    });
    if let Some(group) = group {
        assert!(
            group.controllers[0]
                .table
                .lock()
                .unwrap()
                .registrations
                .is_empty()
        );
    }
}

#[test]
fn shutdown_and_driver_drop_wake_pending_waits_and_preserve_immediate_errors() {
    for drop_driver in [false, true] {
        let (selector, mut workers) = setup(1);
        let socket = packet();
        let mut pending = pin!(read(&selector, &socket));
        assert!(block_on(poll_once(pending.as_mut())).is_none());
        let wake = workers[0].waker();
        wake.wake_by_ref();
        assert_eq!(
            workers[0].turn(Some(Duration::ZERO)).unwrap(),
            selector.readiness.is_none()
        );
        if drop_driver && selector.readiness.is_some() {
            workers.clear();
        } else {
            selector.close();
        }
        assert_eq!(
            block_on(pending).0.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        // A retained wake remains harmless after driver retirement.
        wake.wake_by_ref();
        if let Some(worker) = workers.first_mut() {
            assert!(worker.turn(None).unwrap());
        }
    }
    let (selector, _workers) = setup(1);
    let socket = NativeSocket::new(
        socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap(),
    )
    .unwrap();
    assert!(block_on(read(&selector, &socket)).0.is_err());
}

#[test]
fn cancellation_releases_one_waiter_without_stopping_others() {
    let (selector, mut workers) = setup(1);
    let socket = packet();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let cancellation = Cancellation::new();
    {
        let mut future = pin!(selector.socket_read(&socket, vec![0; 1], &cancellation));
        assert!(block_on(poll_once(future.as_mut())).is_none());
        cancellation.cancel();
        assert!(matches!(
            block_on(future).0,
            Err(OperationError::Cancelled(_))
        ));
    }
    let mut other = pin!(read(&selector, &socket));
    assert!(block_on(poll_once(other.as_mut())).is_none());
    sender.send_to(b"a", socket.local_addr().unwrap()).unwrap();
    workers[0].turn(Some(Duration::from_secs(1))).unwrap();
    assert_eq!(drive(other, &mut workers[0]).0.unwrap(), 1);
}

#[test]
fn deregistered_keys_are_removed_and_never_reused() {
    let (selector, mut workers) = setup(1);
    let Some(group) = &selector.readiness else {
        return;
    };
    let owner = &group.controllers[0];
    for _ in 0..128 {
        let socket = packet();
        {
            let mut future = pin!(read(&selector, &socket));
            assert!(block_on(poll_once(future.as_mut())).is_none());
        }
        assert_eq!(owner.table.lock().unwrap().registrations.len(), 1);
        block_on(selector.socket_close(socket)).unwrap();
        assert!(owner.table.lock().unwrap().registrations.is_empty());
        workers[0].turn(Some(Duration::ZERO)).unwrap();
    }
    assert_eq!(owner.table.lock().unwrap().next, 128);
}

#[test]
fn fallback_drivers_retire_without_owning_an_event_queue() {
    let (group, drivers) = create_with(2, Cancellation::new(), || Ok(None)).unwrap();
    assert!(group.is_none());
    assert_eq!(drivers.len(), 2);
    for driver in drivers {
        let mut worker = driver.enter();
        worker.waker().wake_by_ref();
        worker.waker().wake();
        worker.close();
        assert!(worker.turn(None).unwrap());
    }
    assert!(
        create_with(1, Cancellation::new(), || Err(
            io::ErrorKind::PermissionDenied.into()
        ))
        .is_err()
    );
    assert!(ReadinessSelector::with_workers(SelectorOptions::default(), 0).is_err());
}

#[test]
fn external_routes_are_round_robin_and_stopped_groups_are_reclaimed() {
    let (first, first_drivers) =
        ReadinessSelector::with_workers(SelectorOptions::default(), 2).unwrap();
    let (second, second_drivers) =
        ReadinessSelector::with_workers(SelectorOptions::default(), 1).unwrap();
    let Some(group) = &first.readiness else {
        return;
    };
    assert!(Arc::ptr_eq(&group.route(), &group.controllers[0]));
    assert!(Arc::ptr_eq(&group.route(), &group.controllers[1]));
    let socket = packet();
    let registration = socket.registrations.get(group, socket.get_ref()).unwrap();
    assert!(!format!("{socket:?}").is_empty());
    registration.direction(Interest::Writable).publish();
    block_on(registration.direction(Interest::Writable).wait(group)).unwrap();
    drop(first_drivers);
    assert_eq!(
        block_on(registration.direction(Interest::Readable).wait(group))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert!(socket.registrations.get(group, socket.get_ref()).is_err());
    let replacement = socket
        .registrations
        .get(second.readiness.as_ref().unwrap(), socket.get_ref())
        .unwrap();
    assert_eq!(socket.registrations.entries.lock().unwrap().len(), 1);
    drop(replacement);
    drop(registration);
    drop(second_drivers);
    socket.into_close()().unwrap();
}

#[cfg(target_os = "macos")]
#[test]
fn driver_interruption_is_retryable_but_failure_wakes_all_waiters() {
    use std::sync::atomic::Ordering;
    let (selector, mut workers) = setup(1);
    let socket = packet();
    let mut pending = pin!(read(&selector, &socket));
    assert!(block_on(poll_once(pending.as_mut())).is_none());
    let group = selector.readiness.as_ref().unwrap();
    let controller = &group.controllers[0];
    controller
        .poller
        .failure
        .store(libc::EINTR, Ordering::Relaxed);
    assert!(!workers[0].turn(None).unwrap());
    assert!(!group.closed.is_cancelled());
    controller
        .poller
        .failure
        .store(libc::EBADF, Ordering::Relaxed);
    assert_eq!(
        workers[0].turn(None).unwrap_err().raw_os_error(),
        Some(libc::EBADF)
    );
    assert_eq!(
        block_on(pending).0.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert!(workers[0].turn(None).unwrap());
    workers[0].waker().wake();
}

#[test]
fn explicit_worker_close_wakes_waits_and_key_exhaustion_never_reuses_a_key() {
    let (selector, mut workers) = setup(1);
    let Some(group) = &selector.readiness else {
        return;
    };
    let socket = packet();
    group.controllers[0].table.lock().unwrap().next = usize::MAX - 1;
    assert!(socket.registrations.get(group, socket.get_ref()).is_err());
    workers[0].close();
    assert!(workers[0].turn(None).unwrap());
}

#[cfg(target_os = "macos")]
#[test]
fn failed_driver_creation_and_registration_return_the_os_error() {
    kqueue::INITIAL_FAILURE.set(libc::ENOMEM);
    assert!(
        matches!(create(1, Cancellation::new()), Err(error) if error.raw_os_error() == Some(libc::ENOMEM))
    );
    kqueue::INITIAL_FAILURE.set(libc::ENOMEM);
    assert!(
        matches!(ReadinessSelector::with_workers(SelectorOptions::default(), 1), Err(error) if error.raw_os_error() == Some(libc::ENOMEM))
    );
    let (selector, _workers) = setup(1);
    let socket = packet();
    selector.readiness.as_ref().unwrap().controllers[0]
        .poller
        .failure
        .store(libc::ENOMEM, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        block_on(read(&selector, &socket)).0.unwrap_err().kind(),
        io::ErrorKind::OutOfMemory
    );
    // A failed registration retains no key/record and permits a later attempt.
    assert!(socket.registrations.entries.lock().unwrap().is_empty());
}

#[test]
fn simultaneous_listeners_have_distinct_wakers_and_drop_unregisters_only_one() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};
    #[derive(Default)]
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (selector, _workers) = setup(1);
    let Some(group) = &selector.readiness else {
        return;
    };
    let direction = Direction::new();
    let first = Arc::new(Count::default());
    let second = Arc::new(Count::default());
    let abandoned = Arc::new(Count::default());
    let first_waker = Waker::from(Arc::clone(&first));
    let second_waker = Waker::from(Arc::clone(&second));
    let abandoned_waker = Waker::from(Arc::clone(&abandoned));
    let mut one = Box::pin(direction.wait(group));
    let mut two = Box::pin(direction.wait(group));
    let mut three = Box::pin(direction.wait(group));
    assert!(
        one.as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        two.as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    assert!(
        three
            .as_mut()
            .poll(&mut Context::from_waker(&abandoned_waker))
            .is_pending()
    );
    drop(three);
    direction.publish();
    assert_eq!(first.0.load(Ordering::Relaxed), 1);
    assert_eq!(second.0.load(Ordering::Relaxed), 1);
    assert_eq!(abandoned.0.load(Ordering::Relaxed), 0);
    assert!(
        one.as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_ready()
    );
    assert!(
        two.as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_ready()
    );
}

#[test]
fn events_after_table_retirement_do_not_notify_the_old_registration() {
    let (selector, mut workers) = setup(1);
    let Some(group) = &selector.readiness else {
        return;
    };
    let socket = packet();
    let registration = socket.registrations.get(group, socket.get_ref()).unwrap();
    // Deregistration removes routing before deleting the kernel filter. Model
    // that supported window deterministically while the worker collects an edge.
    let retired = Arc::make_mut(&mut registration.owner.table.lock().unwrap().registrations)
        .remove(&registration.key);
    assert!(retired.is_some());
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sender
        .send_to(b"retired", socket.local_addr().unwrap())
        .unwrap();
    assert!(!workers[0].turn(Some(Duration::from_secs(1))).unwrap());
    assert_eq!(registration.readable.snapshot(), 0);
    assert_eq!(registration.writable.snapshot(), 0);
    drop(registration);
    block_on(selector.socket_close(socket)).unwrap();
}
