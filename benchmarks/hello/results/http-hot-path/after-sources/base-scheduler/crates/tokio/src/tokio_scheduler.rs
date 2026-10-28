// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    BlockingTask, SystemFileMetadata, TokioFile, TokioOptions,
    blocking_pool::{BlockingPool, admission_error},
    file_io,
};
use base_scheduler::{
    Blocking, BlockingError, BufferResult, Cancellation, FileClose, FileOpen, FileOperation,
    FileOptions, FileSeek, FileStat, FileSync, FileTruncate, IoBuf, OperationResult, Spawn,
    SpawnError, SyncMode, TaskError,
};
use futures_channel::oneshot;
use std::{
    io::{self, SeekFrom},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

/// Scheduler capabilities backed by an explicitly selected Tokio runtime.
///
/// The application keeps the runtime alive and driven, with I/O and time enabled. Sockets belong to their registering runtime; files are compatible across adapter instances. Blocking native work and accepted cleanup use independent bounded pools.
pub struct TokioScheduler {
    pub(crate) runtime: tokio::runtime::Handle,
    closed: AtomicBool,
    pub(crate) process_closed: Cancellation,
    pub(crate) work: BlockingPool,
    pub(crate) cleanup: BlockingPool,
    pub(crate) tasks: crate::task_service::TaskService,
    #[cfg(test)]
    pub(crate) timers: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub(crate) option_probe: std::sync::Arc<crate::socket_options::Probe>,
    #[cfg(test)]
    pub(crate) resolver_probe: std::sync::Arc<crate::resolver::Probe>,
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) file_probe: std::sync::Arc<crate::file_extensions::Probe>,
    #[cfg(unix)]
    pub(crate) signals: crate::signal_service::SignalService,
}
impl TokioScheduler {
    /// Adapt an existing runtime using default bounded native-work limits. This does not own or start a Tokio runtime.
    pub fn new(runtime: tokio::runtime::Handle) -> Self {
        Self::with_options(runtime, TokioOptions::default()).expect("default limits are nonzero")
    }
    /// Configure native syscall and cleanup admission independently of Tokio's executor and blocking pool. Both limits must be nonzero.
    pub fn with_options(
        runtime: tokio::runtime::Handle,
        options: TokioOptions,
    ) -> io::Result<Self> {
        if options.threads == 0 || options.capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "thread count and capacity must be nonzero",
            ));
        }
        Ok(Self {
            tasks: crate::task_service::TaskService::new(runtime.clone()),
            runtime,
            closed: AtomicBool::new(false),
            process_closed: Cancellation::new(),
            work: BlockingPool::new(options),
            cleanup: BlockingPool::new(TokioOptions {
                threads: 1,
                capacity: options.capacity,
            }),
            #[cfg(test)]
            timers: Default::default(),
            #[cfg(test)]
            option_probe: Default::default(),
            #[cfg(test)]
            resolver_probe: Default::default(),
            #[cfg(all(test, target_os = "linux"))]
            file_probe: Default::default(),
            #[cfg(unix)]
            signals: crate::signal_service::SignalService::new(),
        })
    }
    pub(crate) fn check_open(&self) -> io::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "scheduler is closed",
            ))
        } else {
            Ok(())
        }
    }
    pub(crate) fn check_socket(&self, socket: &crate::TokioSocket) -> io::Result<()> {
        self.check_open()?;
        self.check_socket_compatibility(socket)
    }
    pub(crate) fn check_socket_compatibility(&self, socket: &crate::TokioSocket) -> io::Result<()> {
        if socket.runtime() != self.runtime.id() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket belongs to another Tokio runtime",
            ));
        }
        Ok(())
    }
    /// Close admission without cancelling accepted socket or clock operations. Existing owning task handles remain valid; callers use their explicit cancellation tokens to stop pending operations. Accepted native I/O and cleanup retire normally, while queued general blocking jobs can be discarded, native process notifications are interrupted, and signal delivery stops.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.process_closed.cancel();
        self.tasks.close();
        self.work.close();
        self.cleanup.close();
        #[cfg(unix)]
        self.signals.close();
    }
    /// Close admission and await owned tasks and all accepted native work and cleanup. The Tokio runtime must remain alive and driven throughout shutdown.
    ///
    /// Coordinate cooperative cancellation and await socket and clock operations before shutdown. These operations are owned by their calling futures and are not separately tracked; shutdown waits for them only through tasks spawned by this adapter. A task cannot await shutdown of the scheduler that must drain it. An uncancelled task or running synchronous work can prevent shutdown indefinitely. This never shuts down the application's Tokio runtime.
    pub async fn shutdown(&self) {
        self.close();
        self.tasks.drain().await;
        self.work.drain().await;
        self.cleanup.drain().await;
        #[cfg(unix)]
        self.signals.drain().await;
    }

    pub(crate) async fn buffer_operation<B: IoBuf>(
        &self,
        file: &TokioFile,
        buffer: B,
        cancellation: &Cancellation,
        operation: impl FnOnce(&std::fs::File, &mut B) -> io::Result<usize> + Send + 'static,
    ) -> BufferResult<B> {
        let (result, (_, buffer)) = self
            .work
            .execute(
                (file.retain(), buffer),
                cancellation,
                move |(file, buffer)| operation(file, buffer),
            )
            .await;
        (result, buffer)
    }

    async fn submit_close(&self, file: TokioFile) -> io::Result<CloseCompletion> {
        let file = file.into_file();
        self.cleanup
            .submit(move || file_io::close(file))
            .await
            .map_err(|error| io::Error::from(admission_error(error)))
    }
}

impl Drop for TokioScheduler {
    fn drop(&mut self) {
        self.close();
    }
}

impl Spawn for TokioScheduler {
    type Handle<Output: Send + 'static> = crate::TokioTask<Output>;
    fn spawn<F>(&self, future: F) -> Result<Self::Handle<F::Output>, SpawnError>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(future)
    }
}

impl Blocking for TokioScheduler {
    type Handle<Output>
        = BlockingTask<Output>
    where
        Output: Send + 'static;

    async fn spawn_blocking<F, Output>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<Self::Handle<Output>, BlockingError>
    where
        F: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        self.work.spawn(operation, cancellation).await
    }
}

impl FileOperation for TokioScheduler {
    type File = TokioFile;
}

impl FileOpen for TokioScheduler {
    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<TokioFile> {
        let path = path.to_owned();
        self.work
            .execute((), cancellation, move |_| {
                std::fs::OpenOptions::new()
                    .read(options.read)
                    .write(options.write)
                    .create(options.create)
                    .create_new(options.create_new)
                    .truncate(options.truncate)
                    .open(path)
                    .map(TokioFile::from_std)
            })
            .await
            .0
    }
}

impl FileSeek for TokioScheduler {
    async fn file_seek(
        &self,
        file: &TokioFile,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        self.work
            .execute(file.retain(), cancellation, move |file| {
                file_io::seek(file, position)
            })
            .await
            .0
    }
}

impl FileTruncate for TokioScheduler {
    async fn file_truncate(
        &self,
        file: &TokioFile,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.work
            .execute(file.retain(), cancellation, move |file| {
                file_io::truncate(file, length)
            })
            .await
            .0
    }
}

impl FileSync for TokioScheduler {
    async fn file_sync(
        &self,
        file: &TokioFile,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.work
            .execute(file.retain(), cancellation, move |file| {
                file_io::sync(file, mode)
            })
            .await
            .0
    }
}

impl FileStat for TokioScheduler {
    type Metadata = SystemFileMetadata;

    async fn file_stat(
        &self,
        file: &TokioFile,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Metadata> {
        self.work
            .execute(file.retain(), cancellation, |file| file_io::stat(file))
            .await
            .0
    }
}

impl FileClose for TokioScheduler {
    async fn file_close(&self, file: TokioFile) -> io::Result<()> {
        wait_close(self.submit_close(file).await?).await
    }

    async fn file_close_submit(&self, file: TokioFile) -> io::Result<()> {
        self.submit_close(file).await.map(drop)
    }
}

type CloseCompletion = oneshot::Receiver<Result<io::Result<()>, TaskError>>;

pub(crate) async fn wait_close(completion: CloseCompletion) -> io::Result<()> {
    completion
        .await
        .map_err(io::Error::other)?
        .map_err(|error| io::Error::other(error.to_string()))?
}

#[cfg(test)]
mod tests;
