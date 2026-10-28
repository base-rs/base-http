// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{
    Blocking, Cancellation, FileClose, FileMetadata, FileOpen, FileOptions, FileStat,
};
use base_scheduler_conformance::{blocking, file};
use base_scheduler_tokio::{TokioOptions, TokioScheduler};
use futures_lite::future::block_on;
use std::{sync::mpsc, time::Duration};

fn with_options(options: TokioOptions) -> std::io::Result<TokioScheduler> {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    });
    TokioScheduler::with_options(runtime.handle().clone(), options)
}

fn run(test: impl FnOnce() + Send + 'static) {
    let (done, completion) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(test)));
    });
    if let Err(payload) = completion
        .recv_timeout(Duration::from_secs(20))
        .expect("conformance test deadline exceeded")
    {
        std::panic::resume_unwind(payload);
    }
}

struct FileFixture {
    scheduler: TokioScheduler,
    directory: tempfile::TempDir,
    path: std::path::PathBuf,
}
impl base_scheduler_conformance::FileTestFixture for FileFixture {
    type Scheduler = TokioScheduler;
    fn scheduler(&self) -> &TokioScheduler {
        &self.scheduler
    }
    fn path(&self) -> &std::path::Path {
        &self.path
    }
    fn assert_absent(&self) {
        assert!(!self.path.exists());
    }
    fn assert_contents(&self, expected: &[u8]) {
        assert_eq!(std::fs::read(&self.path).unwrap(), expected);
    }
}
fn run_file(test: impl AsyncFnOnce(&FileFixture) + Send + 'static) {
    run(move || {
        let directory = tempfile::tempdir().unwrap();
        let fixture = FileFixture {
            scheduler: with_options(TokioOptions::default()).unwrap(),
            path: directory.path().join("file"),
            directory,
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.scheduler.shutdown());
        // Keep the temporary directory until every accepted operation has retired, including after a failed assertion.
        drop(fixture.directory);
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod files {
    use super::*;
    use base_scheduler_conformance as conformance;
    // Public macros must resolve support paths even when a caller shadows `std`.
    mod std {}
    conformance::file_tests!(run: run_file);
    conformance::file_resource_tests!(run: run_file);
    conformance::file_submission_tests!(run: run_file);
}

#[test]
fn files_move_between_threads_and_compatible_scheduler_instances() {
    run(|| {
        block_on(async {
            let first = with_options(TokioOptions::default()).unwrap();
            let second = with_options(TokioOptions {
                threads: 1,
                capacity: 1,
            })
            .unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("file");
            std::fs::write(&path, b"abcdef").unwrap();
            let file = first
                .file_open(&path, FileOptions::READ, &Cancellation::never())
                .await
                .unwrap();
            std::thread::scope(|scope| {
                scope
                    .spawn(|| block_on(file::read(&second, &file, b"abcdef")))
                    .join()
                    .unwrap();
            });
            let metadata = second
                .file_stat(&file, &Cancellation::never())
                .await
                .unwrap();
            assert_eq!(metadata.size(), Some(6));
            second.file_close(file).await.unwrap();
            first.shutdown().await;
            second.shutdown().await;
        })
    });
}

#[cfg(unix)]
#[test]
fn unix_positioned_operations_preserve_the_shared_cursor() {
    use base_scheduler::FileSeek;
    use std::io::SeekFrom;

    block_on(async {
        let scheduler = with_options(TokioOptions::default()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("file");
        std::fs::write(&path, b"abcdef").unwrap();
        let file = scheduler
            .file_open(
                &path,
                FileOptions {
                    write: true,
                    ..FileOptions::READ
                },
                &Cancellation::never(),
            )
            .await
            .unwrap();
        scheduler
            .file_seek(&file, SeekFrom::Start(3), &Cancellation::never())
            .await
            .unwrap();
        file::read_at(&scheduler, &file, 0, b"abcdef").await;
        file::write_at(&scheduler, &file, 0, b"ABC").await;
        assert_eq!(std::fs::read(&path).unwrap(), b"ABCdef");
        assert_eq!(
            scheduler
                .file_seek(&file, SeekFrom::Current(0), &Cancellation::never())
                .await
                .unwrap(),
            3
        );
        scheduler.file_close(file).await.unwrap();
        scheduler.shutdown().await;
    });
}

struct QueueFixture(TokioScheduler);
struct Hold {
    release: Option<mpsc::Sender<()>>,
    _handle: base_scheduler_tokio::BlockingTask<()>,
}
impl Drop for Hold {
    fn drop(&mut self) {
        let _ = self.release.take().unwrap().send(());
    }
}
impl blocking::BlockingFixture for QueueFixture {
    type Scheduler = TokioScheduler;
    type Guard = Hold;
    fn scheduler(&self) -> &Self::Scheduler {
        &self.0
    }
    async fn hold_workers(&self) -> Hold {
        let (started, start) = futures_channel::oneshot::channel();
        let (release, released) = mpsc::channel();
        let handle = self
            .0
            .spawn_blocking(
                move || {
                    started.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(10)).unwrap();
                },
                &Cancellation::never(),
            )
            .await
            .unwrap();
        start.await.unwrap();
        Hold {
            release: Some(release),
            _handle: handle,
        }
    }
}

fn run_blocking(test: impl AsyncFnOnce(&TokioScheduler) + Send + 'static) {
    run(move || {
        let scheduler = with_options(TokioOptions::default()).unwrap();
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&scheduler))));
        block_on(scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
fn run_queue(test: impl AsyncFnOnce(&QueueFixture) + Send + 'static) {
    run(move || {
        let fixture = QueueFixture(
            with_options(TokioOptions {
                threads: 1,
                capacity: 2,
            })
            .unwrap(),
        );
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.0.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod blocking_work {
    use super::*;
    base_scheduler_conformance::blocking_tests!(run: run_blocking);
}
mod blocking_queue {
    use super::*;
    base_scheduler_conformance::blocking_queue_tests!(run: run_queue);
}
