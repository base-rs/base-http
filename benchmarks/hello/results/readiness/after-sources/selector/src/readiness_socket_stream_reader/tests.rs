// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{ReadinessWorker, SelectorDriver, SelectorOptions, SelectorWorker};
use futures_lite::future::{block_on, poll_once};
use std::{
    io::Write,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

fn setup() -> (
    ReadinessSelector,
    ReadinessWorker,
    NativeSocket,
    std::net::TcpStream,
) {
    let (selector, drivers) =
        ReadinessSelector::with_workers(SelectorOptions::default(), 1).unwrap();
    let worker = drivers.into_iter().next().unwrap().enter();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    peer.set_nodelay(true).unwrap();
    let (socket, _) = listener.accept().unwrap();
    (
        selector,
        worker,
        NativeSocket::new(socket.into()).unwrap(),
        peer,
    )
}

fn drive<F: Future>(future: F, worker: &mut ReadinessWorker) -> F::Output {
    let mut future = pin!(future);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(result) = block_on(poll_once(future.as_mut())) {
            return result;
        }
        assert!(Instant::now() < deadline, "stream read stalled");
        worker.turn(Some(Duration::from_millis(10))).unwrap();
    }
}

fn prime(
    reader: &mut ReadinessSocketStreamReader<'_>,
    peer: &mut std::net::TcpStream,
    worker: &mut ReadinessWorker,
) {
    let never = Cancellation::never();
    let mut buffer = [0; 8];
    let mut reading = Box::pin(reader.read_into(&mut buffer, &never));
    assert!(block_on(poll_once(&mut reading)).is_none());
    peer.write_all(b"x").unwrap();
    assert_eq!(drive(reading, worker).unwrap(), 1);
    assert_eq!(buffer[0], b'x');
}

#[test]
fn short_reads_wait_without_a_syscall_and_full_reads_keep_ready() {
    let (selector, mut worker, socket, mut peer) = setup();
    if selector.readiness.is_none() {
        return;
    }
    let never = Cancellation::never();
    let mut reader = ReadinessSocketStreamReader::new(&selector, &socket).unwrap();
    // Initial ready progress still needs no registration.
    peer.write_all(b"a").unwrap();
    assert_eq!(
        drive(reader.read_into(&mut [0; 8], &never), &mut worker).unwrap(),
        1
    );
    prime(&mut reader, &mut peer, &mut worker);
    let registration = reader.registration.as_ref().unwrap().clone();
    assert_eq!(registration.direction(Interest::Readable).snapshot() & 1, 0);

    let calls = AtomicUsize::new(0);
    let mut buffer = [0; 4];
    {
        let mut reading = Box::pin(reader.read_with(buffer.len(), &never, |socket| {
            calls.fetch_add(1, Ordering::Relaxed);
            socket_io::recv_poll(socket, &mut buffer, SocketRecvFlags::default())
        }));
        assert!(block_on(poll_once(&mut reading)).is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        peer.write_all(b"abcdefgh").unwrap();
        assert_eq!(drive(reading, &mut worker).unwrap(), 4);
    }
    assert_eq!(&buffer, b"abcd");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_ne!(registration.direction(Interest::Readable).snapshot() & 1, 0);
    assert_eq!(block_on(reader.read_into(&mut buffer, &never)).unwrap(), 4);
    assert_eq!(&buffer, b"efgh");

    // A full read does not prove exhaustion. One WouldBlock clears the hint.
    let calls = AtomicUsize::new(0);
    {
        let mut reading = Box::pin(reader.read_with(buffer.len(), &never, |socket| {
            calls.fetch_add(1, Ordering::Relaxed);
            socket_io::recv_poll(socket, &mut buffer, SocketRecvFlags::default())
        }));
        assert!(block_on(poll_once(&mut reading)).is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(block_on(poll_once(&mut reading)).is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        peer.write_all(b"z").unwrap();
        assert_eq!(drive(reading, &mut worker).unwrap(), 1);
    }
    assert_eq!(buffer[0], b'z');
    assert!(Arc::ptr_eq(
        reader.registration.as_ref().unwrap(),
        &registration
    ));
    assert!(reader.pool.is_none());

    peer.shutdown(std::net::Shutdown::Write).unwrap();
    assert_eq!(
        drive(reader.read_into(&mut buffer, &never), &mut worker).unwrap(),
        0
    );
    assert!(reader.eof);
    assert_eq!(block_on(reader.read_into(&mut buffer, &never)).unwrap(), 0);
}

#[test]
fn a_readability_event_during_a_short_read_is_not_erased() {
    let (selector, mut worker, socket, mut peer) = setup();
    if selector.readiness.is_none() {
        return;
    }
    let never = Cancellation::never();
    let mut reader = ReadinessSocketStreamReader::new(&selector, &socket).unwrap();
    prime(&mut reader, &mut peer, &mut worker);
    let registration = reader.registration.as_ref().unwrap().clone();
    let direction = registration.direction(Interest::Readable);
    peer.write_all(b"a").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while direction.snapshot() & 1 == 0 {
        assert!(Instant::now() < deadline);
        worker.turn(Some(Duration::from_millis(10))).unwrap();
    }
    let mut buffer = [0; 8];
    let count = block_on(reader.read_with(buffer.len(), &never, |socket| {
        let result = socket_io::recv_poll(socket, &mut buffer, SocketRecvFlags::default());
        // Model delivery of the new edge between recv and its readiness clear.
        peer.write_all(b"b").unwrap();
        direction.publish();
        result
    }))
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(buffer[0], b'a');
    assert_ne!(direction.snapshot() & 1, 0);
    assert_eq!(
        drive(reader.read_into(&mut buffer, &never), &mut worker).unwrap(),
        1
    );
    assert_eq!(buffer[0], b'b');
    assert_eq!(direction.snapshot() & 1, 0);
}

#[test]
fn cancellation_shutdown_and_immediate_errors_preserve_stream_state() {
    let (selector, mut worker, socket, mut peer) = setup();
    if selector.readiness.is_none() {
        return;
    }
    let mut reader = ReadinessSocketStreamReader::new(&selector, &socket).unwrap();
    let cancellation = Cancellation::new();
    let mut buffer = [0x5a; 8];
    {
        let mut reading = Box::pin(reader.read_into(&mut buffer, &cancellation));
        assert!(block_on(poll_once(&mut reading)).is_none());
        cancellation.cancel();
        assert!(matches!(
            block_on(reading),
            Err(base_scheduler::OperationError::Cancelled(_))
        ));
    }
    assert_eq!(buffer, [0x5a; 8]);
    assert!(!reader.established);
    prime(&mut reader, &mut peer, &mut worker);
    // A terminal syscall error is returned without another readiness wait.
    reader
        .registration
        .as_ref()
        .unwrap()
        .direction(Interest::Readable)
        .publish();
    assert_eq!(
        block_on(reader.read_with(8, &Cancellation::never(), |_| Err(
            io::ErrorKind::PermissionDenied.into()
        )))
        .unwrap_err()
        .kind(),
        io::ErrorKind::PermissionDenied
    );
    reader
        .registration
        .as_ref()
        .unwrap()
        .direction(Interest::Readable)
        .clear(
            reader
                .registration
                .as_ref()
                .unwrap()
                .direction(Interest::Readable)
                .snapshot(),
        );
    let never = Cancellation::never();
    let mut reading = Box::pin(reader.read_into(&mut buffer, &never));
    assert!(block_on(poll_once(&mut reading)).is_none());
    selector.close();
    assert_eq!(
        block_on(reading).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );

    let (selector, _worker, socket, _peer) = setup();
    let mut reader = ReadinessSocketStreamReader::new(&selector, &socket).unwrap();
    // Closure between the initial attempt and lazy registration is propagated.
    let error = block_on(reader.read_with(8, &never, |_| {
        selector.close();
        Err(io::ErrorKind::WouldBlock.into())
    }))
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);

    let (selector, _worker, _socket, _peer) = setup();
    let socket = NativeSocket::new(
        socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap(),
    )
    .unwrap();
    let mut reader = ReadinessSocketStreamReader::new(&selector, &socket).unwrap();
    assert!(block_on(reader.read_into(&mut buffer, &never)).is_err());
    assert!(reader.registration.is_none());
}

#[test]
fn data_and_peer_eof_in_one_event_leave_eof_immediately_readable() {
    for shutdown in [std::net::Shutdown::Write, std::net::Shutdown::Both] {
        let (selector, mut worker, socket, mut peer) = setup();
        let never = Cancellation::never();
        let mut reader = ReadinessSocketStreamReader::new(&selector, &socket).unwrap();
        prime(&mut reader, &mut peer, &mut worker);
        let registration = reader.registration.as_ref().unwrap().clone();
        let direction = registration.direction(Interest::Readable);
        peer.write_all(b"bye").unwrap();
        peer.shutdown(shutdown).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while direction.snapshot() & 2 == 0 {
            assert!(Instant::now() < deadline, "peer EOF was not delivered");
            worker.turn(Some(Duration::from_millis(10))).unwrap();
        }
        let mut buffer = [0; 8];
        assert_eq!(block_on(reader.read_into(&mut buffer, &never)).unwrap(), 3);
        assert_eq!(&buffer[..3], b"bye");
        // The same event also reported EOF; no new edge is required to read it.
        assert_eq!(direction.snapshot() & 3, 3);
        let mut reading = pin!(reader.read_into(&mut buffer, &never));
        assert_eq!(block_on(poll_once(reading.as_mut())).unwrap().unwrap(), 0);
    }
}

#[test]
fn urgent_boundaries_keep_trailing_bytes_ready_even_if_an_alias_consumes_urgent_data() {
    for consume_urgent in [false, true] {
        let (selector, mut worker, socket, mut peer) = setup();
        let never = Cancellation::never();
        let mut reader = ReadinessSocketStreamReader::new(&selector, &socket).unwrap();
        prime(&mut reader, &mut peer, &mut worker);
        let registration = reader.registration.as_ref().unwrap().clone();
        let direction = registration.direction(Interest::Readable);
        peer.write_all(b"before").unwrap();
        assert_eq!(
            socket2::SockRef::from(&peer)
                .send_with_flags(b"!", libc::MSG_OOB)
                .unwrap(),
            1
        );
        peer.write_all(b"after").unwrap();
        // Capture one event only after all three loopback segments have arrived.
        std::thread::sleep(Duration::from_millis(10));
        if consume_urgent {
            let mut urgent = [std::mem::MaybeUninit::<u8>::uninit(); 1];
            assert_eq!(
                socket
                    .get_ref()
                    .recv_with_flags(&mut urgent, libc::MSG_OOB | libc::MSG_DONTWAIT)
                    .unwrap(),
                1
            );
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while direction.snapshot() & 1 == 0 {
            assert!(Instant::now() < deadline);
            worker.turn(Some(Duration::from_millis(10))).unwrap();
        }
        let mut buffer = [0; 64];
        assert_eq!(block_on(reader.read_into(&mut buffer, &never)).unwrap(), 6);
        assert_eq!(&buffer[..6], b"before");
        // A short read at the urgent mark consumed fewer bytes than kqueue
        // reported. Keep readiness so the queued suffix needs no new peer write.
        assert_ne!(direction.snapshot() & 1, 0);
        {
            let mut reading = pin!(reader.read_into(&mut buffer, &never));
            assert_eq!(block_on(poll_once(reading.as_mut())).unwrap().unwrap(), 5);
        }
        assert_eq!(&buffer[..5], b"after");
    }
}
