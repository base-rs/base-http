// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{
    BufferResult, Cancellation, FileOperation, FileReader, FileReading, IoBufMut, Spawn,
};
use base_scheduler_native::{NativeExecutorOptions, NativeScheduler, ReadinessSelector, Selector};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct MemorySelector {
    closed: Arc<AtomicBool>,
    retired: Arc<AtomicUsize>,
}
impl Selector for MemorySelector {
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
    async fn shutdown(&self) {
        self.close();
        self.retired.fetch_add(1, Ordering::Relaxed);
    }
}
impl FileOperation for MemorySelector {
    type File = [u8; 3];
}
struct MemoryReader([u8; 3]);
impl FileReading for MemorySelector {
    type Reader<'a> = MemoryReader;
    fn file_reader(&self, file: &Self::File) -> std::io::Result<MemoryReader> {
        Ok(MemoryReader(*file))
    }
}
impl FileReader for MemoryReader {
    async fn read<B: IoBufMut>(
        &mut self,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let count = self.0.len().min(buffer.as_ref().len());
        buffer.as_mut()[..count].copy_from_slice(&self.0[..count]);
        (Ok(count), buffer)
    }
    async fn read_at<B: IoBufMut>(
        &mut self,
        buffer: B,
        _: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.read(buffer, cancellation).await
    }
    async fn close(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_partial_selector_keeps_its_own_resource_type_and_needs_no_executor() {
    let closed = Arc::new(AtomicBool::new(false));
    let retired = Arc::new(AtomicUsize::new(0));
    let selector = MemorySelector {
        closed: Arc::clone(&closed),
        retired: Arc::clone(&retired),
    };
    let scheduler = NativeScheduler::with_selector(
        selector,
        NativeExecutorOptions {
            workers: 1,
            ..Default::default()
        },
    )
    .unwrap();
    scheduler.block_on(async {
        let buffer = vec![0; 3];
        let pointer = buffer.as_ptr();
        let (result, buffer) = scheduler
            .file_reader(b"abc")
            .unwrap()
            .read(buffer, &Cancellation::never())
            .await;
        assert_eq!(result.unwrap(), 3);
        assert_eq!(buffer, b"abc");
        assert_eq!(pointer, buffer.as_ptr());
        assert_eq!(scheduler.spawn(async { 42 }).unwrap().await.unwrap(), 42);
        scheduler.shutdown().await;
    });
    assert!(closed.load(Ordering::Acquire));
    assert_eq!(retired.load(Ordering::Relaxed), 1);
}

#[test]
fn failed_composition_closes_the_supplied_selector() {
    let closed = Arc::new(AtomicBool::new(false));
    let selector = MemorySelector {
        closed: Arc::clone(&closed),
        retired: Arc::default(),
    };
    assert!(
        NativeScheduler::with_selector(
            selector,
            NativeExecutorOptions {
                workers: 0,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(closed.load(Ordering::Acquire));
}

#[test]
fn readiness_is_usable_without_native_task_workers() {
    futures_lite::future::block_on(async {
        let selector = ReadinessSelector::new().unwrap();
        let (client, server) = base_scheduler_conformance::socket::stream_pair(
            &selector,
            base_scheduler::SocketDomain::Ipv4,
        )
        .await;
        base_scheduler_conformance::socket::stream(&selector, &client, &server).await;
        selector.shutdown().await;
    });
}

impl base_scheduler::FileAllocate for MemorySelector {
    async fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: base_scheduler::AllocationMode,
        cancellation: &Cancellation,
    ) -> base_scheduler::OperationResult<()> {
        cancellation.check()?;
        assert_eq!(file, b"abc");
        assert_eq!(
            (offset, length, mode),
            (1, 2, base_scheduler::AllocationMode::KeepSize)
        );
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported).into())
    }
}
impl base_scheduler::FileAdvice for MemorySelector {
    async fn file_advise(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: base_scheduler::AdviceMode,
    ) -> std::io::Result<()> {
        assert_eq!(file, b"abc");
        assert_eq!(
            (offset, length, advice),
            (1, 2, base_scheduler::AdviceMode::Sequential)
        );
        Err(std::io::ErrorKind::Unsupported.into())
    }
    async fn file_advise_submit(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: base_scheduler::AdviceMode,
    ) -> std::io::Result<()> {
        self.file_advise(file, offset, length, advice).await
    }
}

#[test]
fn optional_file_capabilities_preserve_custom_resources_arguments_and_errors() {
    use base_scheduler::{AdviceMode, AllocationMode, FileAdvice, FileAllocate};
    let scheduler = NativeScheduler::with_selector(
        MemorySelector {
            closed: Arc::default(),
            retired: Arc::default(),
        },
        NativeExecutorOptions {
            workers: 1,
            ..Default::default()
        },
    )
    .unwrap();
    scheduler.block_on(async {
        assert_eq!(
            scheduler
                .file_allocate(
                    b"abc",
                    1,
                    2,
                    AllocationMode::KeepSize,
                    &Cancellation::never()
                )
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::Unsupported
        );
        assert_eq!(
            scheduler
                .file_advise(b"abc", 1, 2, AdviceMode::Sequential)
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::Unsupported
        );
        assert_eq!(
            scheduler
                .file_advise_submit(b"abc", 1, 2, AdviceMode::Sequential)
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::Unsupported
        );
        scheduler.shutdown().await;
    });
}

#[test]
fn registered_sockets_options_resolution_and_signals_forward_to_the_selector() {
    use base_scheduler::*;
    use std::net::{TcpListener, TcpStream};
    let scheduler = NativeScheduler::new().unwrap();
    scheduler.block_on(async {
        let never = Cancellation::never();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let peer = scheduler
            .register_socket(listener.accept().unwrap().0)
            .unwrap();
        let listener = scheduler.register_listener(listener).unwrap();
        let stream = scheduler.register_socket(stream).unwrap();
        scheduler
            .socket_set_option(&stream, TcpNoDelay, true, &never)
            .await
            .unwrap();
        assert!(
            scheduler
                .socket_get_option(&stream, TcpNoDelay, &never)
                .await
                .unwrap()
        );
        scheduler
            .socket_shutdown_submit(&stream, std::net::Shutdown::Write)
            .await
            .unwrap();
        let (result, _) = scheduler
            .socket_stream_reader(&peer)
            .unwrap()
            .read_owned(vec![0; 1], &never)
            .await;
        assert_eq!(result.unwrap(), 0);
        // EOF observes shutdown's effect, but the admission-only request may still retain its socket. Scheduler shutdown below drains it.
        drop(stream);
        scheduler.socket_close(peer).await.unwrap();
        scheduler.socket_close(listener).await.unwrap();
        base_scheduler_conformance::address_resolution::numeric(&scheduler, SocketDomain::Ipv4)
            .await;
        #[cfg(unix)]
        {
            let target = Cancellation::new();
            let subscription = scheduler
                .signal_subscribe(&[libc::SIGUSR1], target.clone())
                .unwrap();
            assert!(!target.is_cancelled());
            drop(subscription);
        }
        scheduler.shutdown().await;
    });
}

impl base_scheduler::Signals for MemorySelector {
    type Signal = u8;
    type Subscription = Box<dyn base_scheduler::Notify<u8>>;

    fn signal_subscribe<T: base_scheduler::Notify<u8> + 'static>(
        &self,
        signals: &[u8],
        target: T,
    ) -> std::io::Result<Self::Subscription> {
        assert_eq!(signals, [7]);
        Ok(Box::new(target))
    }
}

#[test]
fn custom_signal_capability_preserves_the_selectors_signal_and_subscription_types() {
    use base_scheduler::Signals;
    let scheduler = NativeScheduler::with_selector(
        MemorySelector {
            closed: Arc::default(),
            retired: Arc::default(),
        },
        NativeExecutorOptions {
            workers: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let (target, receiver) = std::sync::mpsc::channel();
    let subscription = scheduler.signal_subscribe(&[7], target).unwrap();
    subscription.notify(7);
    assert_eq!(receiver.try_recv().unwrap(), 7);
    drop(subscription);
    assert_eq!(
        receiver.try_recv().unwrap_err(),
        std::sync::mpsc::TryRecvError::Disconnected
    );
    scheduler.block_on(scheduler.shutdown());
}

#[test]
fn portable_pool_capacity_is_independent_of_worker_count() {
    use base_scheduler::{BufferPoolOptions, BufferPooling};
    for workers in [1, 3] {
        let scheduler = NativeScheduler::with_options(base_scheduler_native::NativeOptions {
            workers,
            ..Default::default()
        })
        .unwrap();
        let options = BufferPoolOptions::new(3, 64);
        assert_eq!(scheduler.buffer_pool(options).unwrap().capacity(), 3);
        scheduler.block_on(scheduler.shutdown());
    }
}
