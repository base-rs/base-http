// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    BlockingTask, NativeFile, SelectorOptions, SystemFileMetadata,
    blocking_pool::{BlockingPool, admission_error},
    file_io,
};
use base_scheduler::{
    Blocking, BlockingError, BufferResult, Cancellation, FileClose, FileOpen, FileOperation,
    FileOptions, FileSeek, FileStat, FileSync, FileTruncate, IoBuf, OperationResult, SyncMode,
    TaskError,
};
use futures_channel::oneshot;
use std::{
    io::{self, SeekFrom},
    path::Path,
};

/// Native I/O capabilities independent of task execution. Sockets use worker-owned or independently driven readiness; files, resolution, and blocking work use bounded worker pools.
///
/// Compatible instances accept the same file and socket types. `new` and `with_options` provide independent reactor progress; `with_workers` supplies caller-owned drivers.
pub struct ReadinessSelector {
    pub(crate) work: BlockingPool,
    pub(crate) cleanup: BlockingPool,
    pub(crate) closed: Cancellation,
    #[cfg(unix)]
    pub(crate) readiness: Option<std::sync::Arc<crate::readiness_driver::DriverGroup>>,
    #[cfg(test)]
    pub(crate) option_probe: std::sync::Arc<crate::socket_options::Probe>,
    #[cfg(test)]
    pub(crate) resolver_probe: std::sync::Arc<crate::resolver::Probe>,
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) file_probe: std::sync::Arc<crate::file_extensions::Probe>,
    #[cfg(unix)]
    pub(crate) signals: crate::signal_service::SignalService,
}

impl ReadinessSelector {
    /// Create lazy I/O services with default limits.
    pub fn new() -> io::Result<Self> {
        Self::with_options(SelectorOptions::default())
    }

    pub(crate) fn check_open(&self) -> io::Result<()> {
        if self.closed.is_cancelled() {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "scheduler is closed",
            ))
        } else {
            Ok(())
        }
    }

    /// Configure lazy I/O and signal service startup. Worker count and capacity must be nonzero.
    pub fn with_options(options: SelectorOptions) -> io::Result<Self> {
        if options.threads == 0 || options.capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker counts and capacities must be nonzero",
            ));
        }
        Ok(Self {
            #[cfg(test)]
            option_probe: Default::default(),
            #[cfg(test)]
            resolver_probe: Default::default(),
            #[cfg(all(test, target_os = "linux"))]
            file_probe: Default::default(),
            #[cfg(unix)]
            signals: crate::signal_service::SignalService::new(),
            closed: Cancellation::new(),
            #[cfg(unix)]
            readiness: None,
            work: BlockingPool::new(options),
            cleanup: BlockingPool::new(SelectorOptions {
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
        self.closed.cancel();
        #[cfg(unix)]
        if let Some(group) = &self.readiness {
            group.close();
        }
        #[cfg(unix)]
        self.signals.close();
    }

    /// Close admission and await retirement of accepted I/O, blocking work, cleanup, and signal delivery, including abandoned results.
    ///
    /// Coordinate application cancellation and cleanup first. A blocking job must not await shutdown of its own selector. A syscall or closure that never returns can prevent shutdown from completing.
    pub async fn shutdown(&self) {
        self.close();
        self.work.drain().await;
        self.cleanup.drain().await;
        #[cfg(unix)]
        self.signals.drain().await;
    }

    /// Compose readiness with one caller-driven poller per task worker.
    ///
    /// The returned drivers must keep progressing through shutdown. Operations
    /// on this selector use its current entered worker, or an external route;
    /// pending operations keep that owner across task migration. Platforms
    /// without edge polling retain independent `async-io` progress instead.
    pub fn with_workers(
        options: SelectorOptions,
        workers: usize,
    ) -> io::Result<(Self, Vec<crate::ReadinessDriver>)> {
        if workers == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker count must be nonzero",
            ));
        }
        let selector = Self::with_options(options)?;
        #[cfg(unix)]
        let (selector, drivers) = {
            let mut selector = selector;
            let (group, drivers) =
                crate::readiness_driver::create(workers, selector.closed.clone())?;
            selector.readiness = group;
            (selector, drivers)
        };
        #[cfg(not(unix))]
        let drivers = crate::readiness_driver::create(workers);
        Ok((selector, drivers))
    }

    pub(crate) async fn buffer_operation<B: IoBuf>(
        &self,
        file: &NativeFile,
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

    async fn submit_close(&self, file: NativeFile) -> io::Result<CloseCompletion> {
        let file = file.into_file();
        self.cleanup
            .submit(move || file_io::close(file))
            .await
            .map_err(|error| io::Error::from(admission_error(error)))
    }
}

impl Drop for ReadinessSelector {
    fn drop(&mut self) {
        self.close();
    }
}

impl Blocking for ReadinessSelector {
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

impl FileOperation for ReadinessSelector {
    type File = NativeFile;
}

impl FileOpen for ReadinessSelector {
    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<NativeFile> {
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
                    .map(NativeFile::from_std)
            })
            .await
            .0
    }
}

impl FileSeek for ReadinessSelector {
    async fn file_seek(
        &self,
        file: &NativeFile,
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

impl FileTruncate for ReadinessSelector {
    async fn file_truncate(
        &self,
        file: &NativeFile,
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

impl FileSync for ReadinessSelector {
    async fn file_sync(
        &self,
        file: &NativeFile,
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

impl FileStat for ReadinessSelector {
    type Metadata = SystemFileMetadata;

    async fn file_stat(
        &self,
        file: &NativeFile,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Metadata> {
        self.work
            .execute(file.retain(), cancellation, |file| file_io::stat(file))
            .await
            .0
    }
}

impl FileClose for ReadinessSelector {
    async fn file_close(&self, file: NativeFile) -> io::Result<()> {
        wait_close(self.submit_close(file).await?).await
    }

    async fn file_close_submit(&self, file: NativeFile) -> io::Result<()> {
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

impl crate::Selector for ReadinessSelector {
    fn close(&self) {
        self.close();
    }
    async fn shutdown(&self) {
        self.shutdown().await;
    }
}
