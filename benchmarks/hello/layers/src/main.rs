//! Layer controls: fixed work, matched futures, separately timed setup and cleanup.
use base_scheduler::{
    Cancellation, SocketRegistration, SocketStreamReader, SocketStreamReading, SocketStreamWriter,
    SocketStreamWriting, Spawn,
};
use base_scheduler_native::{NativeExecutorOptions, NativeOptions, NativeScheduler};
use base_scheduler_selector::{ReadinessSelector, SelectorDriver, SelectorOptions, SelectorWorker};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    future::Future,
    hint::black_box,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
struct Counter(AtomicUsize);
static READ_FUTURE_BYTES: AtomicUsize = AtomicUsize::new(0);
static WRITE_FUTURE_BYTES: AtomicUsize = AtomicUsize::new(0);
impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
struct Yielding {
    remaining: u64,
}
impl Future for Yielding {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.remaining == 0 {
            return Poll::Ready(());
        }
        self.remaining -= 1;
        black_box(self.remaining);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
async fn supervised(iterations: u64, width: usize) -> u64 {
    let mut futures: FuturesUnordered<_> = (0..width)
        .map(|_| Yielding {
            remaining: iterations,
        })
        .collect();
    let start = Instant::now();
    let mut completed = 0;
    while futures.next().await.is_some() {
        completed += 1;
    }
    assert_eq!(completed, width);
    start.elapsed().as_nanos() as u64
}
fn runtime(io: bool) -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    if io {
        builder.enable_io();
    }
    builder.build().unwrap()
}
fn tasks(backend: &str, kind: &str, iterations: u64, width: usize) -> u64 {
    let is_supervisor = kind == "supervisor";
    if backend == "manual" {
        let mut future = pin!(supervised(iterations, width));
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(elapsed) = future.as_mut().poll(&mut cx) {
                return elapsed;
            }
        }
    }
    if backend == "iouring" {
        let (selector, driver) =
            base_scheduler_iouring::IoUringSelector::new(Default::default()).unwrap();
        let scheduler = NativeScheduler::with_drivers(
            selector,
            vec![driver],
            NativeExecutorOptions {
                workers: 1,
                timer_capacity: 1024,
            },
        )
        .unwrap();
        return native_tasks(scheduler, kind, iterations, width);
    }
    if backend.starts_with("base") {
        let scheduler = if backend == "base-noio" {
            NativeScheduler::with_selector(
                ReadinessSelector::new().unwrap(),
                NativeExecutorOptions {
                    workers: 1,
                    timer_capacity: 1024,
                },
            )
            .unwrap()
        } else {
            NativeScheduler::with_options(NativeOptions {
                workers: 1,
                ..Default::default()
            })
            .unwrap()
        };
        native_tasks(scheduler, kind, iterations, width)
    } else {
        let rt = runtime(backend != "tokio-noio");
        rt.block_on(async {
            tokio::spawn(async move {
                if is_supervisor {
                    supervised(iterations, width).await
                } else {
                    let mut tasks = Vec::new();
                    let start = Instant::now();
                    for _ in 0..width {
                        tasks.push(tokio::spawn(Yielding {
                            remaining: iterations,
                        }));
                    }
                    for task in tasks {
                        task.await.unwrap();
                    }
                    start.elapsed().as_nanos() as u64
                }
            })
            .await
            .unwrap()
        })
    }
}

fn native_tasks<S: base_scheduler_selector::Selector + 'static>(
    scheduler: NativeScheduler<S>,
    kind: &str,
    iterations: u64,
    width: usize,
) -> u64 {
    let scheduler = Arc::new(scheduler);
    let elapsed = if kind == "supervisor" {
        let task = scheduler.spawn(supervised(iterations, width)).unwrap();
        scheduler.block_on(task).unwrap()
    } else {
        let owner = scheduler.clone();
        let task = scheduler
            .spawn(async move {
                let start = Instant::now();
                let tasks: Vec<_> = (0..width)
                    .map(|_| {
                        owner
                            .spawn(Yielding {
                                remaining: iterations,
                            })
                            .unwrap()
                    })
                    .collect();
                for task in tasks {
                    task.await.unwrap();
                }
                start.elapsed().as_nanos() as u64
            })
            .unwrap();
        scheduler.block_on(task).unwrap()
    };
    futures_lite::future::block_on(scheduler.shutdown());
    elapsed
}
fn mio_probe(kind: &str, iterations: u64, width: usize) -> (u64, usize) {
    use std::{os::fd::AsRawFd, os::unix::net::UnixStream};
    let mut poller = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(1024);
    if kind == "empty-poll" {
        let start = Instant::now();
        for _ in 0..iterations {
            poller.poll(&mut events, Some(Duration::ZERO)).unwrap();
            assert!(events.is_empty());
        }
        return (start.elapsed().as_nanos() as u64, 0);
    }
    let mut sockets = Vec::new();
    let mut peers = Vec::new();
    for key in 0..width {
        let (socket, peer) = UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        poller
            .registry()
            .register(
                &mut mio::unix::SourceFd(&socket.as_raw_fd()),
                mio::Token(key),
                mio::Interest::READABLE,
            )
            .unwrap();
        sockets.push(socket);
        peers.push(peer);
    }
    let mut elapsed = 0;
    let mut checksum = 0;
    for _ in 0..iterations {
        for peer in &mut peers {
            peer.write_all(&[1]).unwrap();
        }
        let start = Instant::now();
        poller.poll(&mut events, Some(Duration::ZERO)).unwrap();
        assert_eq!(events.iter().count(), width);
        for event in &events {
            assert!(event.is_readable());
            checksum += black_box(event.token().0);
        }
        elapsed += start.elapsed().as_nanos() as u64;
        for socket in &mut sockets {
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).unwrap(), 1);
        }
    }
    (elapsed, checksum)
}
fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = listener.accept().unwrap().0;
    socket.set_nodelay(true).unwrap();
    peer.set_nodelay(true).unwrap();
    (socket, peer)
}
fn ready_io(backend: &str, write: bool, iterations: u64) -> u64 {
    const BATCH: usize = 512;
    let (mut socket, mut peer) = pair();
    let mut elapsed = 0;
    let mut bytes = [7u8; BATCH];
    if backend == "std" {
        socket.set_nonblocking(true).unwrap();
        for _ in 0..iterations {
            if !write {
                peer.write_all(&bytes).unwrap();
            }
            let start = Instant::now();
            for _ in 0..BATCH {
                let mut byte = [0];
                let count = if write {
                    socket.write(&[7]).unwrap()
                } else {
                    socket.read(&mut byte).unwrap()
                };
                assert_eq!(count, 1);
                if !write {
                    assert_eq!(byte[0], 7);
                }
            }
            elapsed += start.elapsed().as_nanos() as u64;
            if write {
                peer.read_exact(&mut bytes).unwrap();
            }
        }
    } else if backend == "base" {
        let (selector, drivers) =
            ReadinessSelector::with_workers(SelectorOptions::default(), 1).unwrap();
        let mut worker = drivers.into_iter().next().unwrap().enter();
        elapsed = bound_ready_io(&selector, &mut worker, socket, peer, write, iterations);
        worker.close();
    } else if backend == "iouring" {
        let (selector, driver) =
            base_scheduler_iouring::IoUringSelector::new(Default::default()).unwrap();
        let mut worker = driver.enter();
        elapsed = bound_ready_io(&selector, &mut worker, socket, peer, write, iterations);
        worker.close();
        let deadline = Instant::now() + Duration::from_secs(3);
        while !worker.turn(Some(Duration::from_millis(10))).unwrap() {
            assert!(Instant::now() < deadline);
        }
    } else {
        let rt = runtime(true);
        let _entered = rt.enter();
        socket.set_nonblocking(true).unwrap();
        let mut socket = tokio::net::TcpStream::from_std(socket).unwrap();
        {
            let mut byte = [0];
            READ_FUTURE_BYTES.store(
                std::mem::size_of_val(&socket.read(&mut byte)),
                Ordering::Relaxed,
            );
            WRITE_FUTURE_BYTES.store(
                std::mem::size_of_val(&socket.write(&[7])),
                Ordering::Relaxed,
            );
        }
        // Prime the registration outside the measured intervals.
        if write {
            rt.block_on(socket.writable()).unwrap();
        } else {
            peer.write_all(&[7]).unwrap();
            rt.block_on(socket.readable()).unwrap();
            assert_eq!(socket.try_read(&mut [0]).unwrap(), 1);
        }
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..iterations {
            if !write {
                peer.write_all(&bytes).unwrap();
            }
            let start = Instant::now();
            for _ in 0..BATCH {
                let mut byte = [0];
                let count = if write {
                    let mut future = pin!(socket.write(&[7]));
                    match future.as_mut().poll(&mut cx) {
                        Poll::Ready(v) => v.unwrap(),
                        Poll::Pending => panic!("ready write blocked"),
                    }
                } else {
                    let mut future = pin!(socket.read(&mut byte));
                    match future.as_mut().poll(&mut cx) {
                        Poll::Ready(v) => v.unwrap(),
                        Poll::Pending => panic!("ready read blocked"),
                    }
                };
                assert_eq!(count, 1);
                if !write {
                    assert_eq!(byte[0], 7);
                }
            }
            elapsed += start.elapsed().as_nanos() as u64;
            if write {
                peer.read_exact(&mut bytes).unwrap();
            }
        }
    }
    elapsed
}

fn drive<F: Future>(future: F, worker: &mut impl SelectorWorker) -> F::Output {
    let mut future = pin!(future);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        assert!(Instant::now() < deadline, "retirement stalled");
        worker.turn(Some(Duration::from_millis(10))).unwrap();
    }
}
fn bound_ready_io<S: SocketRegistration + SocketStreamReading + SocketStreamWriting>(
    selector: &S,
    worker: &mut impl SelectorWorker,
    socket: TcpStream,
    mut peer: TcpStream,
    write: bool,
    iterations: u64,
) -> u64
where
    S::Socket: 'static,
{
    const BATCH: usize = 512;
    let mut bytes = [7u8; BATCH];
    let never = Cancellation::never();
    let mut elapsed = 0;
    let socket = selector.register_socket(socket).unwrap();
    let mut reader = selector.socket_stream_reader(&socket).unwrap();
    let mut writer = selector.socket_stream_writer(&socket).unwrap();
    {
        let mut byte = [0];
        READ_FUTURE_BYTES.store(
            std::mem::size_of_val(&reader.read_into(&mut byte, &never)),
            Ordering::Relaxed,
        );
        WRITE_FUTURE_BYTES.store(
            std::mem::size_of_val(&writer.write(&[7], &never)),
            Ordering::Relaxed,
        );
    }
    let mut cx = Context::from_waker(Waker::noop());
    if !write {
        let mut byte = [0];
        let mut reading = pin!(reader.read_into(&mut byte, &never));
        assert!(reading.as_mut().poll(&mut cx).is_pending());
        peer.write_all(&[7]).unwrap();
        worker.turn(Some(Duration::from_millis(100))).unwrap();
        assert!(matches!(reading.as_mut().poll(&mut cx), Poll::Ready(Ok(1))));
    }
    for _ in 0..iterations {
        if !write {
            peer.write_all(&bytes).unwrap();
        }
        let start = Instant::now();
        for _ in 0..BATCH {
            let mut byte = [0];
            let count = if write {
                let mut future = pin!(writer.write(&[7], &never));
                match future.as_mut().poll(&mut cx) {
                    Poll::Ready(v) => v.unwrap(),
                    Poll::Pending => panic!("ready write blocked"),
                }
            } else {
                let mut future = pin!(reader.read_into(&mut byte, &never));
                match future.as_mut().poll(&mut cx) {
                    Poll::Ready(v) => v.unwrap(),
                    Poll::Pending => panic!("ready read blocked"),
                }
            };
            assert_eq!(count, 1);
            if !write {
                assert_eq!(byte[0], 7);
            }
        }
        elapsed += start.elapsed().as_nanos() as u64;
        if write {
            peer.read_exact(&mut bytes).unwrap();
        }
    }
    drive(reader.close(), worker).unwrap();
    drive(writer.close(), worker).unwrap();

    elapsed
}
fn short_read(backend: &str, iterations: u64) -> u64 {
    let (mut socket, mut peer) = pair();
    let payload = [7u8; 41];
    let mut buffer = [0u8; 16 * 1024];
    let mut elapsed = 0;
    if backend == "std" {
        socket.set_nonblocking(true).unwrap();
        for _ in 0..iterations {
            peer.write_all(&payload).unwrap();
            let start = Instant::now();
            let count = socket.read(&mut buffer).unwrap();
            elapsed += start.elapsed().as_nanos() as u64;
            assert_eq!(count, payload.len());
            assert_eq!(&buffer[..count], &payload);
        }
    } else if backend == "base" {
        let (selector, drivers) =
            ReadinessSelector::with_workers(SelectorOptions::default(), 1).unwrap();
        let mut worker = drivers.into_iter().next().unwrap().enter();
        let socket = selector.register_socket(socket).unwrap();
        let mut reader = selector.socket_stream_reader(&socket).unwrap();
        let never = Cancellation::never();
        let mut cx = Context::from_waker(Waker::noop());
        {
            let mut reading = pin!(reader.read_into(&mut buffer, &never));
            assert!(reading.as_mut().poll(&mut cx).is_pending());
            peer.write_all(&payload).unwrap();
            worker.turn(Some(Duration::from_millis(100))).unwrap();
            assert!(matches!(
                reading.as_mut().poll(&mut cx),
                Poll::Ready(Ok(41))
            ));
        }
        for _ in 0..iterations {
            peer.write_all(&payload).unwrap();
            worker.turn(Some(Duration::ZERO)).unwrap();
            let start = Instant::now();
            let count = {
                let mut reading = pin!(reader.read_into(&mut buffer, &never));
                match reading.as_mut().poll(&mut cx) {
                    Poll::Ready(v) => v.unwrap(),
                    _ => panic!("short read blocked"),
                }
            };
            elapsed += start.elapsed().as_nanos() as u64;
            assert_eq!(count, payload.len());
            assert_eq!(&buffer[..count], &payload);
        }
        futures_lite::future::block_on(reader.close()).unwrap();
        worker.close();
    } else {
        let rt = runtime(true);
        let _entered = rt.enter();
        socket.set_nonblocking(true).unwrap();
        let mut socket = tokio::net::TcpStream::from_std(socket).unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..iterations {
            peer.write_all(&payload).unwrap();
            rt.block_on(socket.readable()).unwrap();
            let start = Instant::now();
            let count = {
                let mut reading = pin!(socket.read(&mut buffer));
                match reading.as_mut().poll(&mut cx) {
                    Poll::Ready(v) => v.unwrap(),
                    _ => panic!("short read blocked"),
                }
            };
            elapsed += start.elapsed().as_nanos() as u64;
            assert_eq!(count, payload.len());
            assert_eq!(&buffer[..count], &payload);
        }
    }
    elapsed
}
fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 5, "usage: probe BACKEND KIND ITERATIONS WIDTH");
    let backend = &args[1];
    let kind = &args[2];
    let iterations: u64 = args[3].parse().unwrap();
    let width: usize = args[4].parse().unwrap();
    assert!(iterations > 0 && width > 0);
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(counter.clone());
    let mut operations = iterations * width as u64;
    let (ns, checksum) = if kind == "tasks" || kind == "supervisor" {
        (tasks(backend, kind, iterations, width), 0)
    } else if kind == "ready-short-read" {
        (short_read(backend, iterations), 0)
    } else if kind == "ready-read" || kind == "ready-write" {
        operations = iterations * 512;
        (ready_io(backend, kind == "ready-write", iterations), 0)
    } else if backend == "iouring" && kind == "empty-poll" {
        let (selector, mut driver) =
            base_scheduler_iouring::IoUringSelector::new(Default::default()).unwrap();
        let start = Instant::now();
        for _ in 0..iterations {
            assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
        }
        let elapsed = start.elapsed().as_nanos() as u64;
        selector.close();
        while !driver.turn(Some(Duration::from_millis(10))).unwrap() {}
        (elapsed, 0)
    } else if backend == "base" {
        base_scheduler_selector::layer_probe(kind, iterations, width, &waker)
    } else if backend == "tokio" {
        tokio::layer_probe(kind, iterations, width, &waker)
    } else {
        mio_probe(kind, iterations, width)
    };
    if kind == "readiness" || kind == "readiness-list" || kind == "events-driver" {
        assert_eq!(counter.0.load(Ordering::Relaxed), operations as usize);
    }
    if kind == "events" {
        assert_eq!(checksum, iterations as usize * width * (width - 1) / 2);
    }
    println!(
        "{}",
        serde_json::json!({"backend":backend,"kind":kind,"iterations":iterations,"width":width,"operations":operations,"elapsed_ns":ns,"ns_per_operation":ns as f64/operations as f64,"wakes":counter.0.load(Ordering::Relaxed),"checksum":checksum,"read_future_bytes":READ_FUTURE_BYTES.load(Ordering::Relaxed),"write_future_bytes":WRITE_FUTURE_BYTES.load(Ordering::Relaxed)})
    );
    Ok(())
}
