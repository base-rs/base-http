// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{SelectorDriver, SelectorOptions};
use futures_lite::future::{block_on, poll_once};
use std::io::Read;

fn setup() -> (
    ReadinessSelector,
    crate::ReadinessWorker,
    NativeSocket,
    std::net::TcpStream,
) {
    let (selector, drivers) =
        ReadinessSelector::with_workers(SelectorOptions::default(), 1).unwrap();
    let worker = drivers.into_iter().next().unwrap().enter();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    peer.set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .unwrap();
    let (socket, _) = listener.accept().unwrap();
    (
        selector,
        worker,
        NativeSocket::new(socket.into()).unwrap(),
        peer,
    )
}

#[test]
fn ready_writes_skip_registration_and_blocked_writes_retain_it() {
    let (selector, _worker, socket, mut peer) = setup();
    let never = Cancellation::never();
    let mut writer = ReadinessSocketStreamWriter::<Vec<u8>>::new(&selector, &socket).unwrap();
    assert_eq!(block_on(writer.write(b"first", &never)).unwrap(), 5);
    assert!(writer.registration.is_none());
    let mut received = [0; 5];
    peer.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"first");

    // Registration is shared with a reader, but a ready write still needs no lookup.
    let group = selector.readiness.as_ref().unwrap();
    let registration = socket.registrations.get(group, socket.get_ref()).unwrap();
    assert_eq!(
        block_on(writer.write_vectored(&[IoSlice::new(b"next")], &never)).unwrap(),
        4
    );
    assert!(writer.registration.is_none());
    peer.read_exact(&mut received[..4]).unwrap();
    assert_eq!(&received[..4], b"next");

    let mut attempts = 0;
    assert_eq!(
        block_on(writer.write_with(&never, |socket| {
            attempts += 1;
            if attempts == 1 {
                Err(io::ErrorKind::WouldBlock.into())
            } else {
                socket_io::send_poll(socket, b"retry", SocketSendFlags::default())
            }
        }))
        .unwrap(),
        5
    );
    assert_eq!(attempts, 2);
    assert!(Arc::ptr_eq(
        writer.registration.as_ref().unwrap(),
        &registration
    ));
    peer.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"retry");

    let direction = registration.direction(Interest::Writable);
    direction.publish();
    let mut attempts = 0;
    assert_eq!(
        block_on(writer.write_with(&never, |socket| {
            attempts += 1;
            if attempts == 1 {
                // A newer event delivered during WouldBlock must survive its clear.
                direction.publish();
                Err(io::ErrorKind::WouldBlock.into())
            } else {
                socket_io::send_poll(socket, b"last", SocketSendFlags::default())
            }
        }))
        .unwrap(),
        4
    );
    assert_eq!(attempts, 2);
    assert_ne!(direction.snapshot() & 1, 0);
    peer.read_exact(&mut received[..4]).unwrap();
    assert_eq!(&received[..4], b"last");
}

#[test]
fn pending_writes_observe_cancellation_and_scheduler_closure() {
    let (selector, _worker, socket, _peer) = setup();
    let mut writer = ReadinessSocketStreamWriter::<Vec<u8>>::new(&selector, &socket).unwrap();
    let cancel = Cancellation::new();
    {
        let mut writing =
            pin!(writer.write_with(&cancel, |_| Err(io::ErrorKind::WouldBlock.into())));
        assert!(block_on(poll_once(writing.as_mut())).is_none());
        cancel.cancel();
        assert!(matches!(
            block_on(writing),
            Err(OperationError::Cancelled(_))
        ));
    }
    let registration = writer.registration.as_ref().unwrap().clone();
    assert!(
        block_on(writer.write_with(&Cancellation::never(), |_| Err(
            io::ErrorKind::PermissionDenied.into()
        )))
        .is_err()
    );
    assert!(Arc::ptr_eq(
        writer.registration.as_ref().unwrap(),
        &registration
    ));
    let never = Cancellation::never();
    let mut writing = pin!(writer.write_with(&never, |_| Err(io::ErrorKind::WouldBlock.into())));
    assert!(block_on(poll_once(writing.as_mut())).is_none());
    selector.close();
    assert_eq!(
        block_on(writing).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );

    let (selector, _worker, socket, _peer) = setup();
    let mut writer = ReadinessSocketStreamWriter::<Vec<u8>>::new(&selector, &socket).unwrap();
    assert_eq!(
        block_on(writer.write_with(&never, |_| {
            selector.close();
            Err(io::ErrorKind::WouldBlock.into())
        }))
        .unwrap_err()
        .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn worker_driven_writes_resume_after_real_tcp_backpressure() {
    use crate::SelectorWorker;
    use std::time::{Duration, Instant};
    let (selector, mut worker, socket, mut peer) = setup();
    socket.get_ref().set_send_buffer_size(4096).unwrap();
    socket2::SockRef::from(&peer)
        .set_recv_buffer_size(4096)
        .unwrap();
    let payload = [0x37; 64 * 1024];
    let mut queued = 0;
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        assert!(Instant::now() < deadline, "TCP send queue did not fill");
        match socket_io::send_poll(socket.get_ref(), &payload, SocketSendFlags::default()) {
            Ok(count) => {
                assert_ne!(count, 0);
                queued += count;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("send failed: {error}"),
        }
    }
    let mut writer = ReadinessSocketStreamWriter::<Vec<u8>>::new(&selector, &socket).unwrap();
    let never = Cancellation::never();
    // TCP may free capacity between the saturation probe and the first write.
    // Keep accepted progress in the prefix until a write actually suspends.
    let extra = std::cell::Cell::new(0);
    let resume = std::cell::Cell::new(false);
    let mut writing = pin!(async {
        loop {
            let count = writer.write(&[0x37], &never).await?;
            extra.set(extra.get() + count);
            if resume.get() {
                return Ok::<usize, OperationError>(count);
            }
        }
    });
    assert!(block_on(poll_once(writing.as_mut())).is_none());
    let expected = queued + extra.get() + 1;
    resume.set(true);
    let receiving = std::thread::spawn(move || {
        let mut received = vec![0; expected];
        peer.read_exact(&mut received).unwrap();
        assert!(received.iter().all(|byte| *byte == 0x37));
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(result) = block_on(poll_once(writing.as_mut())) {
            assert_eq!(result.unwrap(), 1);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "worker did not resume the blocked write"
        );
        worker.turn(Some(Duration::from_millis(10))).unwrap();
    }
    receiving.join().unwrap();
}
