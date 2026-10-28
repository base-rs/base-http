// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use base_scheduler::SocketStreamWriter;
use base_scheduler::*;

macro_rules! conformance {
    ($($name:ident),*) => {$ (
        #[test]
        fn $name() {
            crate::test_support::run(|| {
                let scheduler = crate::test_support::scheduler();
                futures_lite::future::block_on(async {
                    let (sender, receiver) = base_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4).await;
                    base_scheduler_conformance::socket_stream_writing::$name(&scheduler, &sender, &receiver).await;
                    scheduler.shutdown().await;
                });
            });
        }
    )*};
}
conformance!(
    ordering,
    independent_progress,
    overlapped_read_write,
    abandoned_admission,
    bound_io
);

#[test]
fn close_pending() {
    crate::test_support::run(|| {
        let scheduler = crate::test_support::scheduler();
        futures_lite::future::block_on(async {
            let (sender, _receiver) =
                base_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            base_scheduler_conformance::socket_stream_writing::close_pending(&scheduler, &sender)
                .await;
            scheduler.shutdown().await;
        });
    });
}

#[test]
fn rejects_datagrams_and_closed_admission() {
    crate::test_support::run(|| {
        let scheduler = crate::test_support::scheduler();
        futures_lite::future::block_on(async {
            let never = Cancellation::never();
            let datagram = scheduler
                .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
                .await
                .unwrap();
            assert_eq!(
                scheduler
                    .socket_stream_output::<Vec<u8>>(&datagram)
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            let (sender, _receiver) =
                base_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            crate::socket_io::SOCKET_TYPE_ERROR
                .with(|slot| slot.set(Some(io::ErrorKind::PermissionDenied)));
            assert_eq!(
                scheduler
                    .socket_stream_output::<Vec<u8>>(&sender)
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::PermissionDenied
            );
            let mut writer = scheduler.socket_stream_output::<Vec<u8>>(&sender).unwrap();
            scheduler.close();
            assert_eq!(
                scheduler.socket_type(&sender).unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                scheduler
                    .socket_stream_output::<Vec<u8>>(&sender)
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            let (error, buffer) = writer.submit(vec![42], &never).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, [42]);
            writer.close().await.unwrap();
            scheduler.shutdown().await;
        });
    });
}

#[test]
fn immediate_error_is_accepted_and_retained() {
    crate::test_support::run(|| {
        let scheduler = crate::test_support::scheduler();
        futures_lite::future::block_on(async {
            let never = Cancellation::never();
            let (sender, _receiver) =
                base_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            scheduler
                .socket_shutdown(&sender, std::net::Shutdown::Write)
                .await
                .unwrap();
            let mut writer = scheduler.socket_stream_output::<Vec<u8>>(&sender).unwrap();
            writer.submit(vec![42], &never).await.unwrap();
            let kind = writer.flush(&never).await.unwrap_err().kind();
            let (error, buffer) = writer.submit(vec![43], &never).await.unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(buffer, [43]);
            assert_eq!(writer.flush(&never).await.unwrap_err().kind(), kind);
            writer.close().await.unwrap();
            scheduler.shutdown().await;
        });
    });
}

#[test]
fn accepted_cancellation_and_drop_release_owners() {
    crate::test_support::run(|| {
        let scheduler = crate::test_support::scheduler();
        futures_lite::future::block_on(async {
            let never = Cancellation::never();
            let (sender, _receiver) =
                base_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            let pool = HeapBufferPool::new(1, 8 * 1024 * 1024).unwrap();
            let mut writer = scheduler.socket_stream_output(&sender).unwrap();
            let cancellation = Cancellation::new();
            // Keep the retry task queued so cancellation cannot race successful completion.
            let (gate, entered, release) = crate::test_support::Gate::new();
            let blocked = scheduler.tasks.spawn(async move { gate.wait() }).unwrap();
            entered
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            inject([Err(io::ErrorKind::WouldBlock.into())]);
            writer
                .submit(pool.acquire(&never).await.unwrap(), &cancellation)
                .await
                .map_err(|(e, _)| e)
                .unwrap();
            assert!(pool.try_acquire().is_none());
            cancellation.cancel();
            drop(release);
            blocked.await.unwrap();
            assert_cancelled(writer.flush(&never).await);
            assert!(pool.try_acquire().is_some());
            writer.close().await.unwrap();
            drop(writer);
            let mut writer = scheduler.socket_stream_output(&sender).unwrap();
            let (gate, entered, release) = crate::test_support::Gate::new();
            let blocked = scheduler.tasks.spawn(async move { gate.wait() }).unwrap();
            entered
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            inject([Err(io::ErrorKind::WouldBlock.into())]);
            writer
                .submit(pool.acquire(&never).await.unwrap(), &never)
                .await
                .map_err(|(e, _)| e)
                .unwrap();
            drop(writer);
            assert!(pool.try_acquire().is_none());
            drop(release);
            blocked.await.unwrap();
            drop(pool.acquire(&never).await.unwrap());
            scheduler.shutdown().await;
        });
    });
}

#[test]
fn ready_submission_and_flush_allocate_nothing() {
    crate::test_support::run(|| {
        let scheduler = crate::test_support::scheduler();
        futures_lite::future::block_on(async {
            let never = Cancellation::never();
            let (sender, _receiver) =
                base_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            let mut writer = scheduler.socket_stream_output(&sender).unwrap();
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            {
                let mut submission = std::pin::pin!(writer.submit(vec![42], &never));
                let allocations = allocation_counter::measure(|| {
                    assert_eq!(
                        submission
                            .as_mut()
                            .poll(&mut context)
                            .map(|result| result.is_ok()),
                        Poll::Ready(true)
                    );
                });
                assert_eq!(allocations.count_total, 0);
            }
            {
                let mut flush = std::pin::pin!(writer.flush(&never));
                let allocations = allocation_counter::measure(|| {
                    assert_eq!(
                        flush
                            .as_mut()
                            .poll(&mut context)
                            .map(|result| result.is_ok()),
                        Poll::Ready(true)
                    );
                });
                assert_eq!(allocations.count_total, 0);
            }
            writer.close().await.unwrap();
            scheduler.shutdown().await;
        });
    });
}

thread_local! {
    static SEND_RESULTS: std::cell::RefCell<std::collections::VecDeque<io::Result<usize>>> = const { std::cell::RefCell::new(std::collections::VecDeque::new()) };
    static SEND_BLOCKED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn send_result() -> Option<io::Result<usize>> {
    SEND_RESULTS
        .with(|results| results.borrow_mut().pop_front())
        .or_else(|| {
            SEND_BLOCKED
                .with(|blocked| blocked.get().then(|| Err(io::ErrorKind::WouldBlock.into())))
        })
}

struct BlockSends;
impl BlockSends {
    fn new() -> Self {
        SEND_BLOCKED.with(|blocked| assert!(!blocked.replace(true)));
        Self
    }
}
impl Drop for BlockSends {
    fn drop(&mut self) {
        SEND_BLOCKED.with(|blocked| blocked.set(false));
    }
}

fn inject(results: impl IntoIterator<Item = io::Result<usize>>) {
    SEND_RESULTS.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(slot.is_empty());
        slot.extend(results);
    });
}

#[test]
fn partial_initial_sends_continue_from_the_accepted_offset() {
    crate::test_support::run(|| {
        let scheduler = crate::test_support::scheduler();
        futures_lite::future::block_on(async {
            let never = Cancellation::never();
            let (sender, receiver) =
                base_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            // Deliver the prefix represented by the injected partial syscall result.
            assert_eq!(
                scheduler
                    .socket_send_poll(&sender, b"a", SocketSendFlags::default(), &never)
                    .await
                    .unwrap(),
                1
            );
            let mut writer = scheduler.socket_stream_output(&sender).unwrap();
            inject([Ok(1)]);
            writer.submit(b"abc".to_vec(), &never).await.unwrap();
            writer.flush(&never).await.unwrap();
            let mut incoming = [0; 3];
            let mut received = 0;
            while received < incoming.len() {
                let count = scheduler
                    .socket_recv_poll(
                        &receiver,
                        &mut incoming[received..],
                        SocketRecvFlags::default(),
                        &never,
                    )
                    .await
                    .unwrap();
                assert!(count > 0);
                received += count;
            }
            assert_eq!(&incoming, b"abc");
            writer.close().await.unwrap();
            scheduler.shutdown().await;
        });
    });
}

#[test]
fn syscall_and_task_admission_failures_are_retained_after_acceptance() {
    crate::test_support::run(|| {
        let scheduler = crate::test_support::scheduler();
        futures_lite::future::block_on(async {
            let never = Cancellation::never();
            let (sender, _receiver) =
                base_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            let mut writer = scheduler.socket_stream_output(&sender).unwrap();
            inject([Ok(0)]);
            writer.submit(vec![42], &never).await.unwrap();
            assert_eq!(
                writer.flush(&never).await.unwrap_err().kind(),
                io::ErrorKind::WriteZero
            );
            writer.close().await.unwrap();
            let mut writer = scheduler.socket_stream_output(&sender).unwrap();
            inject([Err(io::ErrorKind::WouldBlock.into())]);
            writer.submit(vec![43], &never).await.unwrap();
            writer.flush(&never).await.unwrap();
            writer.close().await.unwrap();
            let mut writer = scheduler.socket_stream_output(&sender).unwrap();
            scheduler.tasks.close();
            inject([Err(io::ErrorKind::Interrupted.into())]);
            writer.submit(vec![44], &never).await.unwrap();
            assert_eq!(
                writer.flush(&never).await.unwrap_err().kind(),
                io::ErrorKind::Other
            );
            writer.close().await.unwrap();
            scheduler.shutdown().await;
        });
    });
}

#[test]
fn background_retry_errors_and_pending_close_preserve_the_result() {
    crate::test_support::run(|| {
        let scheduler = crate::test_support::scheduler();
        futures_lite::future::block_on(async {
            let never = Cancellation::never();
            let stopping = Cancellation::new();
            let (sender, _receiver) =
                base_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            let mut buffer = vec![42; 8 * 1024 * 1024];
            inject([Err(io::ErrorKind::Interrupted.into()), Ok(0)]);
            assert_eq!(
                write_remaining(
                    &sender.0,
                    &mut buffer,
                    0,
                    &scheduler.socket_retry,
                    &never,
                    &stopping
                )
                .await
                .unwrap_err()
                .kind(),
                io::ErrorKind::WriteZero
            );
            inject([Err(io::ErrorKind::PermissionDenied.into())]);
            assert_eq!(
                write_remaining(
                    &sender.0,
                    &mut buffer,
                    0,
                    &scheduler.socket_retry,
                    &never,
                    &stopping
                )
                .await
                .unwrap_err()
                .kind(),
                io::ErrorKind::PermissionDenied
            );
            {
                // Retry pulses may race the first poll, so every send must remain blocked until stop.
                let _blocked = BlockSends::new();
                let mut pending = std::pin::pin!(write_remaining(
                    &sender.0,
                    &mut buffer,
                    0,
                    &scheduler.socket_retry,
                    &never,
                    &stopping
                ));
                assert!(
                    futures_lite::future::poll_once(pending.as_mut())
                        .await
                        .is_none()
                );
                stopping.cancel();
                assert_cancelled(pending.await);
            }
            // A close requested before the background task starts must also stop without sending.
            assert_cancelled(
                write_remaining(
                    &sender.0,
                    &mut buffer,
                    0,
                    &scheduler.socket_retry,
                    &never,
                    &stopping,
                )
                .await,
            );
            scheduler.shutdown().await;
        });
    });
}

struct RetiringBuffer {
    bytes: Vec<u8>,
    retired: Option<Box<dyn FnOnce() + Send>>,
}
impl AsRef<[u8]> for RetiringBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
// SAFETY: The vector's stable allocation remains exclusively owned through retirement.
unsafe impl IoBuf for RetiringBuffer {}
impl Drop for RetiringBuffer {
    fn drop(&mut self) {
        if let Some(retired) = self.retired.take() {
            retired();
        }
    }
}

#[test]
fn admission_rechecks_cancellation_and_shutdown_after_retiring_the_previous_owner() {
    crate::test_support::run(|| {
        for close_scheduler in [false, true] {
            let scheduler = std::sync::Arc::new(crate::test_support::scheduler());
            futures_lite::future::block_on(async {
                let never = Cancellation::never();
                let cancellation = Cancellation::new();
                let (sender, receiver) = base_scheduler_conformance::socket::stream_pair(
                    &*scheduler,
                    SocketDomain::Ipv4,
                )
                .await;
                sender.0.set_send_buffer_size(4096).unwrap();
                let peer = receiver.0.try_clone().unwrap();
                peer.set_nonblocking(false).unwrap();
                peer.set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let action: Box<dyn FnOnce() + Send> = if close_scheduler {
                    let scheduler = scheduler.clone();
                    Box::new(move || scheduler.close())
                } else {
                    let cancellation = cancellation.clone();
                    Box::new(move || {
                        cancellation.cancel();
                    })
                };
                let mut writer = scheduler.socket_stream_output(&sender).unwrap();
                // Force background ownership and hold its worker until the next admission waits.
                let (gate, entered, release) = crate::test_support::Gate::new();
                let blocked = scheduler.tasks.spawn(async move { gate.wait() }).unwrap();
                entered
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                inject([Err(io::ErrorKind::WouldBlock.into())]);
                writer
                    .submit(
                        RetiringBuffer {
                            bytes: vec![6; 8 * 1024 * 1024],
                            retired: Some(action),
                        },
                        &never,
                    )
                    .await
                    .map_err(|(error, _)| error)
                    .unwrap();
                let (error, returned) = {
                    let mut next = std::pin::pin!(writer.submit(
                        RetiringBuffer {
                            bytes: vec![7],
                            retired: None
                        },
                        &cancellation
                    ));
                    assert!(
                        futures_lite::future::poll_once(next.as_mut())
                            .await
                            .is_none()
                    );
                    let reader = std::thread::spawn(move || {
                        let mut peer = std::net::TcpStream::from(peer);
                        let mut bytes = vec![0; 8 * 1024 * 1024];
                        std::io::Read::read_exact(&mut peer, &mut bytes).unwrap();
                        assert!(bytes.iter().all(|byte| *byte == 6));
                    });
                    drop(release);
                    blocked.await.unwrap();
                    scheduler.tasks.drain().await;
                    scheduler.tasks.wait(false).await;
                    let result = next.await.err().unwrap();
                    reader.join().unwrap();
                    result
                };
                assert_eq!(returned.bytes, [7]);
                if close_scheduler {
                    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
                } else {
                    assert_cancelled(Err(error));
                }
                writer.close().await.unwrap();
                scheduler.shutdown().await;
            });
        }
    });
}

fn assert_cancelled(result: OperationResult<()>) {
    assert_eq!(
        std::mem::discriminant(&result.unwrap_err()),
        std::mem::discriminant(&OperationError::Cancelled(Cancelled))
    );
}

#[test]
fn every_write_form_waits_for_accepted_output_and_preserves_terminal_errors() {
    crate::test_support::run(|| {
        let scheduler = DirectScheduler::new(Default::default()).unwrap();
        futures_lite::future::block_on(async {
            let never = Cancellation::never();
            let cancelled = Cancellation::new();
            cancelled.cancel();
            let (sender, receiver) =
                base_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            let mut writer = scheduler.socket_stream_output::<Vec<u8>>(&sender).unwrap();
            writer.state.accept(
                scheduler
                    .tasks
                    .spawn(async { Err(io::Error::from(io::ErrorKind::ConnectionReset).into()) })
                    .unwrap(),
            );
            let (error, returned) = writer.submit(vec![42], &never).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
            assert_eq!(returned, [42]);
            writer.close().await.unwrap();
            for mode in 0..5 {
                for waiting in [false, true] {
                    let mut writer = scheduler.socket_stream_output::<Vec<u8>>(&sender).unwrap();
                    if waiting {
                        let stopping = writer.state.cancellation();
                        let task = scheduler
                            .tasks
                            .spawn(async move {
                                stopping.cancelled().await;
                                Err(base_scheduler::Cancelled.into())
                            })
                            .unwrap();
                        writer.state.accept(task);
                    } else {
                        writer
                            .state
                            .fail(io::Error::from(io::ErrorKind::ConnectionReset).into());
                    }
                    let result = match mode {
                        0 => writer.write(b"x", &cancelled).await,
                        1 => {
                            writer
                                .write_vectored(&[std::io::IoSlice::new(b"x")], &cancelled)
                                .await
                        }
                        2 => {
                            writer
                                .write_with_flags(b"x", SocketSendFlags::default(), &cancelled)
                                .await
                        }
                        3 => writer.write_owned(vec![1], &cancelled).await.0,
                        _ => {
                            writer
                                .write_owned_vectored(
                                    [vec![1]],
                                    SocketSendFlags::default(),
                                    &cancelled,
                                )
                                .await
                                .0
                        }
                    };
                    assert_eq!(
                        result.unwrap_err().kind(),
                        if waiting {
                            io::ErrorKind::Interrupted
                        } else {
                            io::ErrorKind::ConnectionReset
                        }
                    );
                    writer.close().await.unwrap();
                }
                let mut writer = scheduler.socket_stream_output::<Vec<u8>>(&sender).unwrap();
                writer
                    .state
                    .accept(scheduler.tasks.spawn(async { Ok(()) }).unwrap());
                let result = match mode {
                    0 => writer.write(b"x", &never).await,
                    1 => {
                        writer
                            .write_vectored(&[std::io::IoSlice::new(b"x")], &never)
                            .await
                    }
                    2 => {
                        writer
                            .write_with_flags(b"x", SocketSendFlags::default(), &never)
                            .await
                    }
                    3 => writer.write_owned(b"x".to_vec(), &never).await.0,
                    _ => {
                        writer
                            .write_owned_vectored(
                                [b"x".to_vec()],
                                SocketSendFlags::default(),
                                &never,
                            )
                            .await
                            .0
                    }
                };
                assert_eq!(result.unwrap(), 1);
                let mut byte = [0];
                assert_eq!(
                    scheduler
                        .socket_recv_poll(
                            &receiver,
                            &mut byte,
                            base_scheduler::SocketRecvFlags::default(),
                            &never
                        )
                        .await
                        .unwrap(),
                    1
                );
                assert_eq!(byte, *b"x");
                writer.close().await.unwrap();
            }
            scheduler.shutdown().await;
        });
    });
}
