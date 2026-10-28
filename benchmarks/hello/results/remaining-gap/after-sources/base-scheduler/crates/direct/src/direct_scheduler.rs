// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    BlockingTask, DirectFile, DirectOptions, SystemFileMetadata,
    blocking_pool::{BlockingPool, admission_error},
    file_io,
    socket_retry::SocketRetry,
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
};

/// Explicit task, clock, file, socket, and blocking-work services, independent of the executor polling their operation futures.
///
/// All direct instances accept the same file and socket resources. Ordinary operations use a bounded worker pool; cleanup uses an independent bounded worker. Tasks have separate polling workers, timers have a dedicated driver, and Unix signal callbacks run on a separate dispatch thread. Nonblocking socket attempts release capacity before waiting to retry.
pub struct DirectScheduler {
    pub(crate) work: BlockingPool,
    pub(crate) cleanup: BlockingPool,
    pub(crate) socket_retry: std::sync::Arc<SocketRetry>,
    pub(crate) tasks: crate::task_service::TaskService,
    pub(crate) timers: crate::timer_service::TimerService,
    #[cfg(test)]
    pub(crate) option_probe: std::sync::Arc<crate::socket_options::Probe>,
    #[cfg(test)]
    pub(crate) resolver_probe: std::sync::Arc<crate::resolver::Probe>,
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) file_probe: std::sync::Arc<crate::file_extensions::Probe>,
    #[cfg(unix)]
    pub(crate) signals: crate::signal_service::SignalService,
}

impl DirectScheduler {
    /// Start task polling workers and configure lazy I/O, timer, and signal worker startup. Counts and capacity must be nonzero.
    pub fn new(options: DirectOptions) -> io::Result<Self> {
        if options.threads == 0 || options.capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "thread count and capacity must be nonzero",
            ));
        }
        Ok(Self {
            tasks: crate::task_service::TaskService::new(options.threads)?,
            timers: crate::timer_service::TimerService::new(),
            #[cfg(test)]
            option_probe: Default::default(),
            #[cfg(test)]
            resolver_probe: Default::default(),
            #[cfg(all(test, target_os = "linux"))]
            file_probe: Default::default(),
            #[cfg(unix)]
            signals: crate::signal_service::SignalService::new(),
            socket_retry: std::sync::Arc::new(SocketRetry::new()),
            work: BlockingPool::new(options),
            cleanup: BlockingPool::new(DirectOptions {
                threads: 1,
                capacity: options.capacity,
            }),
        })
    }

    /// Stop admission. Queued general blocking jobs are discarded; accepted I/O and cleanup retire normally. Running closures cannot be interrupted.
    ///
    /// Dropping the scheduler also closes admission without blocking the dropping thread. Workers retain outstanding resources until access ends.
    pub fn close(&self) {
        self.work.close();
        self.cleanup.close();
        self.socket_retry.close();
        self.tasks.close();
        self.timers.close();
        #[cfg(unix)]
        self.signals.close();
    }

    /// Close admission and await retirement of all accepted work and tasks, including abandoned results. Pending timers fail and Unix signal delivery stops.
    ///
    /// Coordinate application cancellation and asynchronous cleanup before calling this. Waiting for shutdown from a blocking job owned by this scheduler would wait for that same job. A syscall or closure that never returns can prevent shutdown from completing.
    pub async fn shutdown(&self) {
        self.close();
        self.work.drain().await;
        self.cleanup.drain().await;
        self.tasks.drain().await;
        #[cfg(unix)]
        self.signals.drain().await;
    }

    pub(crate) async fn buffer_operation<B: IoBuf>(
        &self,
        file: &DirectFile,
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

    async fn submit_close(&self, file: DirectFile) -> io::Result<CloseCompletion> {
        let file = file.into_file();
        self.cleanup
            .submit(move || file_io::close(file))
            .await
            .map_err(|error| io::Error::from(admission_error(error)))
    }
}

impl Drop for DirectScheduler {
    fn drop(&mut self) {
        self.close();
    }
}

impl Spawn for DirectScheduler {
    type Handle<Output: Send + 'static> = crate::DirectTask<Output>;
    fn spawn<F>(&self, future: F) -> Result<Self::Handle<F::Output>, SpawnError>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(future)
    }
}

impl Blocking for DirectScheduler {
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

impl FileOperation for DirectScheduler {
    type File = DirectFile;
}

impl FileOpen for DirectScheduler {
    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<DirectFile> {
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
                    .map(DirectFile::from_std)
            })
            .await
            .0
    }
}

impl FileSeek for DirectScheduler {
    async fn file_seek(
        &self,
        file: &DirectFile,
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

impl FileTruncate for DirectScheduler {
    async fn file_truncate(
        &self,
        file: &DirectFile,
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

impl FileSync for DirectScheduler {
    async fn file_sync(
        &self,
        file: &DirectFile,
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

impl FileStat for DirectScheduler {
    type Metadata = SystemFileMetadata;

    async fn file_stat(
        &self,
        file: &DirectFile,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Metadata> {
        self.work
            .execute(file.retain(), cancellation, |file| file_io::stat(file))
            .await
            .0
    }
}

impl FileClose for DirectScheduler {
    async fn file_close(&self, file: DirectFile) -> io::Result<()> {
        wait_close(self.submit_close(file).await?).await
    }

    async fn file_close_submit(&self, file: DirectFile) -> io::Result<()> {
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
