// Appended inside Tokio's private I/O driver only in the disposable probe copy.
pub(super) fn layer_empty(iterations: u64) -> u64 {
    let (mut driver, handle) = Driver::new(1024, None).unwrap();
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        driver.turn(&handle, Some(Duration::ZERO));
    }
    start.elapsed().as_nanos() as u64
}
pub(super) fn layer_events(iterations: u64, width: usize, waker: &std::task::Waker) -> u64 {
    use std::{
        io::{Read, Write},
        os::{fd::AsRawFd, unix::net::UnixStream},
        task::{Context, Poll},
        time::Instant,
    };
    let (mut driver, handle) = Driver::new(1024, None).unwrap();
    let records: Vec<_> = (0..width)
        .map(|_| Box::new(super::ScheduledIo::default()))
        .collect();
    let mut sockets = Vec::new();
    let mut peers = Vec::new();
    for record in &records {
        let (socket, peer) = UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        handle
            .registry
            .register(
                &mut mio::unix::SourceFd(&socket.as_raw_fd()),
                record.token(),
                mio::Interest::READABLE,
            )
            .unwrap();
        sockets.push(socket);
        peers.push(peer);
    }
    let mut cx = Context::from_waker(waker);
    let mut elapsed = 0;
    for _ in 0..iterations {
        for record in &records {
            assert!(record.poll_readiness(&mut cx, Direction::Read).is_pending());
        }
        for peer in &mut peers {
            peer.write_all(&[1]).unwrap();
        }
        let start = Instant::now();
        driver.turn(&handle, Some(Duration::ZERO));
        elapsed += start.elapsed().as_nanos() as u64;
        for record in &records {
            let event = match record.poll_readiness(&mut cx, Direction::Read) {
                Poll::Ready(event) => event,
                _ => panic!("missing event"),
            };
            record.clear_readiness(event);
        }
        for socket in &mut sockets {
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).unwrap(), 1);
        }
    }
    elapsed
}

pub(super) fn layer_dispatch(
    iterations: u64,
    width: usize,
    waiting: bool,
    waker: &std::task::Waker,
) -> u64 {
    use std::{
        io::Write,
        os::{fd::AsRawFd, unix::net::UnixStream},
        task::Context,
        time::Instant,
    };
    let mut poller = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(1024);
    let records: Vec<_> = (0..width)
        .map(|_| Box::new(super::ScheduledIo::default()))
        .collect();
    let mut pairs = Vec::new();
    for record in &records {
        let (socket, mut peer) = UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        poller
            .registry()
            .register(
                &mut mio::unix::SourceFd(&socket.as_raw_fd()),
                record.token(),
                mio::Interest::READABLE,
            )
            .unwrap();
        peer.write_all(&[1]).unwrap();
        pairs.push((socket, peer));
    }
    poller.poll(&mut events, Some(Duration::ZERO)).unwrap();
    assert_eq!(events.iter().count(), width);
    let mut cx = Context::from_waker(waker);
    let mut elapsed = 0;
    for _ in 0..iterations {
        if waiting {
            for record in &records {
                assert!(record.poll_readiness(&mut cx, Direction::Read).is_pending());
            }
        }
        let start = Instant::now();
        for event in &events {
            let token = event.token();
            assert!(token != TOKEN_WAKEUP && token != TOKEN_SIGNAL);
            unsafe {
                dispatch_event(token, Ready::from_mio(event));
            }
        }
        elapsed += start.elapsed().as_nanos() as u64;
        for record in &records {
            record.clear_readiness(record.ready_event(crate::io::Interest::READABLE));
        }
    }
    elapsed
}
