// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(target_os = "linux")]

use base_scheduler::*;
use base_scheduler_conformance::FileTestFixture;
use base_scheduler_iouring::{IoUringOptions, IoUringSelector};
use base_scheduler_native::{NativeExecutorOptions, NativeScheduler};
use futures_lite::future::block_on;
use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    pin::pin,
    sync::mpsc,
    task::{Context, Poll},
    time::Duration,
};

struct Fixture<S> {
    scheduler: S,
    path: PathBuf,
    _directory: tempfile::TempDir,
}
impl<S: base_scheduler_conformance::FileTestScheduler> FileTestFixture for Fixture<S> {
    type Scheduler = S;
    fn scheduler(&self) -> &S {
        &self.scheduler
    }
    fn path(&self) -> &Path {
        &self.path
    }
    fn assert_absent(&self) {
        assert!(!self.path.exists());
    }
    fn assert_contents(&self, expected: &[u8]) {
        assert_eq!(std::fs::read(&self.path).unwrap(), expected);
    }
}
fn fixture<S>(scheduler: S) -> Fixture<S> {
    let directory = tempfile::tempdir().unwrap();
    Fixture {
        scheduler,
        path: directory.path().join("file"),
        _directory: directory,
    }
}
fn watchdog(test: impl FnOnce() + Send + 'static) {
    let (done, completed) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(catch_unwind(AssertUnwindSafe(test)));
    });
    if let Err(panic) = completed
        .recv_timeout(Duration::from_secs(20))
        .expect("selector test timed out")
    {
        std::panic::resume_unwind(panic);
    }
}
fn options() -> IoUringOptions {
    // Each case creates independent rings; keep their locked kernel memory small when the shared conformance suites run many fixtures concurrently.
    IoUringOptions {
        entries: 16,
        ..Default::default()
    }
}
fn run_selector(test: impl AsyncFnOnce(&Fixture<IoUringSelector>) + Send + 'static) {
    watchdog(move || {
        let (selector, driver) = IoUringSelector::new(options()).unwrap();
        let thread = std::thread::spawn(move || driver.run());
        let fixture = fixture(selector);
        let result = catch_unwind(AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.scheduler.shutdown());
        thread.join().unwrap().unwrap();
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}
fn run_native(test: impl AsyncFnOnce(&Fixture<NativeScheduler<IoUringSelector>>) + Send + 'static) {
    watchdog(move || {
        let (selector, driver) = IoUringSelector::new(options()).unwrap();
        let thread = std::thread::spawn(move || driver.run());
        let scheduler = NativeScheduler::with_selector(
            selector,
            NativeExecutorOptions {
                workers: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let fixture = fixture(scheduler);
        let result = catch_unwind(AssertUnwindSafe(|| {
            fixture.scheduler.block_on(test(&fixture))
        }));
        block_on(fixture.scheduler.shutdown());
        thread.join().unwrap().unwrap();
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}
fn run_native_workers(
    test: impl AsyncFnOnce(&Fixture<NativeScheduler<IoUringSelector>>) + Send + 'static,
) {
    watchdog(move || {
        let (selector, drivers) = IoUringSelector::with_workers(options(), 2).unwrap();
        let scheduler = NativeScheduler::with_drivers(
            selector,
            drivers,
            NativeExecutorOptions {
                workers: 2,
                ..Default::default()
            },
        )
        .unwrap();
        let fixture = fixture(scheduler);
        let result = catch_unwind(AssertUnwindSafe(|| {
            fixture.scheduler.block_on(test(&fixture))
        }));
        block_on(fixture.scheduler.shutdown());
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}

macro_rules! ordered_writer_tests {
    ($module:ident, $run:ident) => {
        mod $module {
            use super::*;
            macro_rules! case {
                ($name:ident) => {
                    #[test]
                    fn $name() {
                        $run(async |fixture| {
                            let scheduler = &fixture.scheduler;
                            let (sender, receiver) =
                                base_scheduler_conformance::socket::stream_pair(
                                    scheduler,
                                    SocketDomain::Ipv4,
                                )
                                .await;
                            base_scheduler_conformance::socket_stream_writing::$name(
                                scheduler, &sender, &receiver,
                            )
                            .await;
                            scheduler.socket_close(sender).await.unwrap();
                            scheduler.socket_close(receiver).await.unwrap();
                        });
                    }
                };
            }
            case!(ordering);
            case!(bound_io);
            case!(independent_progress);
            case!(overlapped_read_write);
            case!(abandoned_admission);
            #[test]
            fn close_pending() {
                $run(async |fixture| {
                    let scheduler = &fixture.scheduler;
                    let (sender, receiver) = base_scheduler_conformance::socket::stream_pair(
                        scheduler,
                        SocketDomain::Ipv4,
                    )
                    .await;
                    base_scheduler_conformance::socket_stream_writing::close_pending(
                        scheduler, &sender,
                    )
                    .await;
                    scheduler.socket_close(sender).await.unwrap();
                    scheduler.socket_close(receiver).await.unwrap();
                });
            }
        }
    };
}

ordered_writer_tests!(ordered_writer_selector, run_selector);
ordered_writer_tests!(ordered_writer_native, run_native);
ordered_writer_tests!(ordered_writer_worker_rings, run_native_workers);

mod native_worker_rings {
    use super::*;
    base_scheduler_conformance::file_tests!(run: run_native_workers);
    base_scheduler_conformance::file_resource_tests!(run: run_native_workers);
    base_scheduler_conformance::file_submission_tests!(run: run_native_workers);
}
mod independent_selector {
    use super::*;
    base_scheduler_conformance::file_tests!(run: run_selector);
    base_scheduler_conformance::file_resource_tests!(run: run_selector);
    base_scheduler_conformance::file_submission_tests!(run: run_selector);
}
mod native_composition {
    use super::*;
    base_scheduler_conformance::file_tests!(run: run_native);
    base_scheduler_conformance::file_resource_tests!(run: run_native);
    base_scheduler_conformance::file_submission_tests!(run: run_native);
}

#[test]
fn one_thread_can_poll_a_borrowing_future_and_drive_the_ring() {
    watchdog(|| {
        let (selector, mut driver) = IoUringSelector::new(IoUringOptions::default()).unwrap();
        let waker = driver.waker();
        let mut context = Context::from_waker(&waker);
        let directory = tempfile::tempdir().unwrap();
        let value = std::rc::Rc::new(b"same-thread".to_vec());
        let mut operation = pin!(async {
            let never = Cancellation::never();
            let file = selector
                .file_open(
                    &directory.path().join("file"),
                    FileOptions {
                        read: true,
                        write: true,
                        create: true,
                        ..Default::default()
                    },
                    &never,
                )
                .await
                .unwrap();
            let mut writer = selector.file_writer(&file).unwrap();
            let mut reader = selector.file_reader(&file).unwrap();
            let (result, _) = writer.write_at(value.as_ref().clone(), 0, &never).await;
            assert_eq!(result.unwrap(), value.len());
            let (result, buffer) = reader.read_at(vec![0; value.len()], 0, &never).await;
            assert_eq!(result.unwrap(), value.len());
            assert_eq!(&buffer, value.as_ref());
            reader.close().await.unwrap();
            writer.close().await.unwrap();
            drop((reader, writer));
            selector.file_close(file).await.unwrap();
            selector.shutdown().await;
        });
        loop {
            if let Poll::Ready(()) = operation.as_mut().poll(&mut context) {
                break;
            }
            driver.turn(None).unwrap();
        }
        assert!(driver.turn(None).unwrap());
    });
}

#[test]
fn composed_tasks_clocks_and_socket_completions_share_one_explicit_scheduler() {
    run_native(async |fixture| {
        let scheduler = &fixture.scheduler;
        assert_eq!(scheduler.spawn(async { 42 }).unwrap().await.unwrap(), 42);
        scheduler
            .sleep(Duration::from_millis(1), &Cancellation::never())
            .await
            .unwrap();
        let (sender, receiver) =
            base_scheduler_conformance::socket::stream_pair(scheduler, SocketDomain::Ipv4).await;
        base_scheduler_conformance::socket_messages::stream(scheduler, &sender, &receiver).await;
        base_scheduler_conformance::socket::stream(scheduler, &sender, &receiver).await;
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}

#[test]
fn fallback_capabilities_compose_with_completion_io() {
    run_native(async |fixture| {
        use std::net::{Shutdown, TcpListener, TcpStream};
        let scheduler = &fixture.scheduler;
        let never = Cancellation::never();
        let file = scheduler
            .file_open(
                &fixture.path,
                FileOptions {
                    write: true,
                    create: true,
                    ..FileOptions::READ
                },
                &never,
            )
            .await
            .unwrap();
        scheduler
            .file_allocate(&file, 0, 4096, AllocationMode::Extend, &never)
            .await
            .unwrap();
        assert_eq!(std::fs::metadata(&fixture.path).unwrap().len(), 4096);
        scheduler
            .file_advise(&file, 0, 0, AdviceMode::Sequential)
            .await
            .unwrap();
        scheduler
            .file_advise_submit(&file, 0, 0, AdviceMode::Normal)
            .await
            .unwrap();
        // Submission-only advice retains its file until execution finishes. Ordinary destruction can release our owner while shutdown drains it; exclusive close requires a separate idle handle.
        drop(file);
        let idle_file = scheduler
            .file_open(&fixture.path, FileOptions::READ, &never)
            .await
            .unwrap();
        scheduler.file_close_submit(idle_file).await.unwrap();
        assert_eq!(
            scheduler
                .spawn_blocking(|| 42, &never)
                .await
                .unwrap()
                .await
                .unwrap(),
            42
        );
        base_scheduler_conformance::address_resolution::numeric(scheduler, SocketDomain::Ipv4)
            .await;
        let subscription = scheduler
            .signal_subscribe(&[libc::SIGUSR1], Cancellation::new())
            .unwrap();
        drop(subscription);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let server = scheduler
            .register_socket(listener.accept().unwrap().0)
            .unwrap();
        let listener = scheduler.register_listener(listener).unwrap();
        let client = scheduler.register_socket(client).unwrap();
        scheduler
            .socket_set_option(&client, TcpNoDelay, true, &never)
            .await
            .unwrap();
        assert!(
            scheduler
                .socket_get_option(&client, TcpNoDelay, &never)
                .await
                .unwrap()
        );
        base_scheduler_conformance::socket_poll::scalar_transfer(scheduler, &client, &server).await;
        base_scheduler_conformance::socket_poll::message_transfer(scheduler, &client, &server)
            .await;
        scheduler
            .socket_shutdown_submit(&client, Shutdown::Write)
            .await
            .unwrap();
        let mut byte = [0];
        let mut reader = scheduler.socket_stream_reader(&server).unwrap();
        assert_eq!(reader.read_into(&mut byte, &never).await.unwrap(), 0);
        reader.close().await.unwrap();
        drop(reader);
        // Observing peer EOF does not guarantee that the submission-only shutdown has released its retained socket owner yet.
        drop(client);
        scheduler.socket_close(server).await.unwrap();
        scheduler.socket_close_submit(listener).await.unwrap();
    });
}

macro_rules! socket_suites {
    ($name:ident, $run:ident, $scheduler:ty, $domain:expr) => {
        mod $name {
            use super::*;
            use base_scheduler_selector::NativeSocket;
            fn streams(test: impl AsyncFnOnce(&$scheduler, &NativeSocket, &NativeSocket) + Send + 'static) {
                $run(async move |fixture| {
                    let scheduler = &fixture.scheduler;
                    let (sender, receiver) = base_scheduler_conformance::socket::stream_pair(scheduler, $domain).await;
                    test(scheduler, &sender, &receiver).await;
                    scheduler.socket_close(sender).await.unwrap();
                    scheduler.socket_close(receiver).await.unwrap();
                });
            }
            fn borrowed_streams(test: impl AsyncFnOnce(&$scheduler, &NativeSocket, &NativeSocket) + Send + 'static) {
                $run(async move |fixture| {
                    let scheduler = &fixture.scheduler;
                    let (sender, receiver) = base_scheduler_conformance::socket::stream_pair(scheduler, $domain).await;
                    test(scheduler, &sender, &receiver).await;
                    // Abandonment tests may leave a readiness-only request retiring. Ordinary destruction is safe; fixture shutdown drains those requests before ending the driver.
                    drop(sender);
                    drop(receiver);
                });
            }
            fn datagrams(test: impl AsyncFnOnce(&$scheduler, &NativeSocket, &NativeSocket) + Send + 'static) {
                $run(async move |fixture| {
                    let scheduler = &fixture.scheduler;
                    let never = Cancellation::never();
                    let address: SocketAddress = match $domain {
                        SocketDomain::Ipv4 => "127.0.0.1:0",
                        SocketDomain::Ipv6 => "[::1]:0",
                    }.parse().unwrap();
                    let sender = scheduler.socket_open($domain, SocketType::Datagram, None, &never).await.unwrap();
                    let receiver = scheduler.socket_open($domain, SocketType::Datagram, None, &never).await.unwrap();
                    scheduler.socket_bind(&sender, address, &never).await.unwrap();
                    scheduler.socket_bind(&receiver, address, &never).await.unwrap();
                    test(scheduler, &sender, &receiver).await;
                    scheduler.socket_close(sender).await.unwrap();
                    scheduler.socket_close(receiver).await.unwrap();
                });
            }
            #[test]
            fn pooled_messages() {
                $run(async move |fixture| {
                    base_scheduler_conformance::socket_receiving::messages(&fixture.scheduler).await;
                });
            }
            #[test]
            fn pooled_stream_eof() {
                $run(async move |fixture| {
                    base_scheduler_conformance::socket_receiving::stream_eof(&fixture.scheduler).await;
                });
            }
            mod scalar { use super::*; base_scheduler_conformance::socket_tests!(run: streams); }
            mod message { use super::*; base_scheduler_conformance::socket_message_tests!(run: streams); }
            mod datagram { use super::*; base_scheduler_conformance::socket_datagram_tests!(run: datagrams); }
            mod borrowed_scalar { use super::*; base_scheduler_conformance::socket_poll_tests!(run: borrowed_streams); }
            mod borrowed_message { use super::*; base_scheduler_conformance::socket_message_poll_tests!(run: borrowed_streams); }
            mod borrowed_datagram { use super::*; base_scheduler_conformance::socket_datagram_poll_tests!(run: datagrams); }
            mod packet_handles { use super::*; base_scheduler_conformance::socket_packet_io_tests!(run: datagrams); }
        }
    };
}
socket_suites!(
    selector_ipv4,
    run_selector,
    IoUringSelector,
    SocketDomain::Ipv4
);
socket_suites!(
    selector_ipv6,
    run_selector,
    IoUringSelector,
    SocketDomain::Ipv6
);
socket_suites!(
    native_ipv4,
    run_native,
    NativeScheduler<IoUringSelector>,
    SocketDomain::Ipv4
);
socket_suites!(
    native_ipv6,
    run_native,
    NativeScheduler<IoUringSelector>,
    SocketDomain::Ipv6
);
socket_suites!(
    workers_ipv4,
    run_native_workers,
    NativeScheduler<IoUringSelector>,
    SocketDomain::Ipv4
);
socket_suites!(
    workers_ipv6,
    run_native_workers,
    NativeScheduler<IoUringSelector>,
    SocketDomain::Ipv6
);

#[test]
fn directional_monitors_work_with_an_independent_driver() {
    run_selector(async |fixture| {
        let scheduler = &fixture.scheduler;
        let (sender, receiver) =
            base_scheduler_conformance::socket::stream_pair(scheduler, SocketDomain::Ipv4).await;
        base_scheduler_conformance::socket_resources::stream(scheduler, &sender, &receiver).await;
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}

#[test]
fn directional_monitors_work_through_the_native_scheduler() {
    run_native(async |fixture| {
        let scheduler = &fixture.scheduler;
        let (sender, receiver) =
            base_scheduler_conformance::socket::stream_pair(scheduler, SocketDomain::Ipv4).await;
        base_scheduler_conformance::socket_resources::stream(scheduler, &sender, &receiver).await;
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}

#[test]
fn pooled_socket_receivers_work_on_worker_owned_rings() {
    watchdog(|| {
        let (selector, drivers) = IoUringSelector::with_workers(options(), 2).unwrap();
        let scheduler = std::sync::Arc::new(
            NativeScheduler::with_drivers(
                selector,
                drivers,
                NativeExecutorOptions {
                    workers: 2,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let owned = scheduler.clone();
        scheduler.block_on(async move {
            let (sender, socket) =
                base_scheduler_conformance::socket::stream_pair(owned.as_ref(), SocketDomain::Ipv4)
                    .await;
            base_scheduler_conformance::socket_receiving::demand_driven(
                owned.as_ref(),
                &sender,
                &socket,
            )
            .await;
            base_scheduler_conformance::socket_receiving::stream(owned.as_ref(), &sender, &socket)
                .await;
            owned.socket_close(sender).await.unwrap();
            owned.socket_close(socket).await.unwrap();
            let never = Cancellation::never();
            let sender = owned
                .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
                .await
                .unwrap();
            let socket = owned
                .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
                .await
                .unwrap();
            owned
                .socket_bind(&sender, "127.0.0.1:0".parse().unwrap(), &never)
                .await
                .unwrap();
            owned
                .socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)
                .await
                .unwrap();
            base_scheduler_conformance::socket_poll::datagram_first_poll(
                owned.as_ref(),
                &sender,
                &socket,
            )
            .await;
            base_scheduler_conformance::socket_receiving::datagram(
                owned.as_ref(),
                &sender,
                &socket,
            )
            .await;
            owned.socket_close(sender).await.unwrap();
            owned.socket_close(socket).await.unwrap();
        });
        scheduler.block_on(scheduler.shutdown());
    });
}
