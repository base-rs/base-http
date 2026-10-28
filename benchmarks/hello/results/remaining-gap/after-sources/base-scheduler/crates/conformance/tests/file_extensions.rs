// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;
use base_scheduler::*;
use base_scheduler_conformance::{
    FileAdviceFixture, FileAllocateFixture, file_advice, file_allocate,
};
use futures_channel::oneshot;
use futures_lite::future::block_on;
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

// Controlled syscall models validate the suites on every target. These are not native allocation/advice implementations and do not certify filesystem guarantees.
struct Data {
    bytes: Vec<u8>,
    cursor: u64,
}
#[derive(Clone)]
struct File {
    data: Arc<Mutex<Data>>,
    read_only: bool,
    failing: bool,
}
impl File {
    fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(Data {
                bytes: b"abcdef".to_vec(),
                cursor: 3,
            })),
            read_only: false,
            failing: false,
        }
    }
}
fn assert_state(file: &File, contents: &[u8], cursor: u64) {
    let data = file.data.lock().unwrap();
    assert_eq!(data.bytes, contents);
    assert_eq!(data.cursor, cursor);
}
fn valid_range(offset: u64, length: u64) -> io::Result<()> {
    if offset
        .checked_add(length)
        .is_none_or(|end| end > i64::MAX as u64)
    {
        Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid range"))
    } else {
        Ok(())
    }
}
#[derive(Clone, Copy, PartialEq)]
enum AllocationFault {
    None,
    IgnoreCancellation,
    NoReservation,
    ChangeCursor,
    LoseKeepSize,
    IgnoreValidation,
    AllowReadOnly,
}
struct Allocator {
    calls: AtomicUsize,
    reserved: Mutex<Vec<(u64, u64)>>,
    fault: AllocationFault,
}
impl FileOperation for Allocator {
    type File = File;
}
impl FileAllocate for Allocator {
    async fn file_allocate(
        &self,
        file: &File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        token: &Cancellation,
    ) -> OperationResult<()> {
        if self.fault != AllocationFault::IgnoreCancellation {
            token.check()?;
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fault != AllocationFault::IgnoreValidation {
            valid_range(offset, length)?;
            if length == 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "zero length").into());
            }
        }
        if file.read_only && self.fault != AllocationFault::AllowReadOnly {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "read only").into());
        }
        if self.fault != AllocationFault::NoReservation {
            self.reserved.lock().unwrap().push((offset, length));
        }
        let mut data = file.data.lock().unwrap();
        if mode == AllocationMode::Extend || self.fault == AllocationFault::LoseKeepSize {
            let end = (offset + length) as usize;
            if end > data.bytes.len() {
                data.bytes.resize(end, 0);
            }
        }
        if self.fault == AllocationFault::ChangeCursor {
            data.cursor = 0;
        }
        Ok(())
    }
}
struct AllocationFixture {
    scheduler: Allocator,
    file: File,
    read_only: File,
}
impl AllocationFixture {
    fn new(fault: AllocationFault) -> Self {
        let file = File::new();
        let read_only = File {
            read_only: true,
            ..file.clone()
        };
        Self {
            scheduler: Allocator {
                calls: AtomicUsize::new(0),
                reserved: Mutex::default(),
                fault,
            },
            file,
            read_only,
        }
    }
}
impl FileAllocateFixture for AllocationFixture {
    type Scheduler = Allocator;
    fn scheduler(&self) -> &Allocator {
        &self.scheduler
    }
    fn file(&self) -> &File {
        &self.file
    }
    fn read_only_file(&self) -> &File {
        &self.read_only
    }
    fn assert_state(&self, contents: &[u8], cursor: u64) {
        assert_state(&self.file, contents, cursor);
    }
    fn assert_reserved(&self, offset: u64, length: u64) {
        assert!(
            self.scheduler
                .reserved
                .lock()
                .unwrap()
                .contains(&(offset, length)),
            "no reservation reached the native boundary"
        );
    }
    fn calls(&self) -> usize {
        self.scheduler.calls.load(Ordering::SeqCst)
    }
}
fn run_allocate(test: impl AsyncFnOnce(&AllocationFixture) + Send + 'static) {
    support::run(move || block_on(test(&AllocationFixture::new(AllocationFault::None))));
}
mod allocation {
    base_scheduler_conformance::file_allocate_tests!(run: super::run_allocate);
}
#[test]
fn rejects_allocation_contract_violations() {
    for fault in [
        AllocationFault::IgnoreCancellation,
        AllocationFault::NoReservation,
        AllocationFault::ChangeCursor,
        AllocationFault::LoseKeepSize,
        AllocationFault::IgnoreValidation,
        AllocationFault::AllowReadOnly,
    ] {
        support::run(move || {
            let fixture = AllocationFixture::new(fault);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                block_on(async {
                    match fault {
                        AllocationFault::IgnoreCancellation => {
                            file_allocate::cancellation(&fixture).await
                        }
                        AllocationFault::LoseKeepSize => file_allocate::keep_size(&fixture).await,
                        AllocationFault::IgnoreValidation | AllocationFault::AllowReadOnly => {
                            file_allocate::errors(&fixture).await
                        }
                        _ => file_allocate::extend(&fixture).await,
                    }
                })
            }));
            assert!(
                result.is_err(),
                "allocation suite accepted an incorrect backend"
            );
        });
    }
}

#[derive(Clone, Copy, PartialEq)]
enum AdviceFault {
    None,
    Eager,
    ChangeCursor,
    WrongRange,
    EarlyCompletion,
    AbandonAccepted,
    IgnoreValidation,
    LoseError,
}
struct Job {
    file: File,
    offset: u64,
    length: u64,
    mode: AdviceMode,
    completion: oneshot::Sender<io::Result<()>>,
}
struct AdviceState {
    held: AtomicBool,
    jobs: Mutex<Vec<Job>>,
    observed: Mutex<Vec<(u64, u64, AdviceMode)>>,
    fault: AdviceFault,
}
impl AdviceState {
    fn drain(&self) {
        if self.held.load(Ordering::SeqCst) {
            return;
        }
        let jobs = std::mem::take(&mut *self.jobs.lock().unwrap());
        for job in jobs {
            if self.fault == AdviceFault::AbandonAccepted && job.completion.is_canceled() {
                continue;
            }
            let offset = if self.fault == AdviceFault::WrongRange {
                0
            } else {
                job.offset
            };
            self.observed
                .lock()
                .unwrap()
                .push((offset, job.length, job.mode));
            if self.fault == AdviceFault::ChangeCursor {
                job.file.data.lock().unwrap().cursor = 0;
            }
            let result = if job.file.failing && self.fault != AdviceFault::LoseError {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "native advice failure",
                ))
            } else {
                Ok(())
            };
            let _ = job.completion.send(result);
        }
    }
    fn enqueue(
        &self,
        file: &File,
        offset: u64,
        length: u64,
        mode: AdviceMode,
    ) -> oneshot::Receiver<io::Result<()>> {
        let (completion, receiver) = oneshot::channel();
        self.jobs.lock().unwrap().push(Job {
            file: file.clone(),
            offset,
            length,
            mode,
            completion,
        });
        self.drain();
        receiver
    }
}
struct Adviser(Arc<AdviceState>);
impl FileOperation for Adviser {
    type File = File;
}
impl FileAdvice for Adviser {
    fn file_advise(
        &self,
        file: &File,
        offset: u64,
        length: u64,
        mode: AdviceMode,
    ) -> impl Future<Output = io::Result<()>> + Send {
        let eager = if self.0.fault == AdviceFault::Eager {
            Some(self.0.enqueue(file, offset, length, mode))
        } else {
            None
        };
        async move {
            if self.0.fault != AdviceFault::IgnoreValidation {
                valid_range(offset, length)?;
            }
            let completion = eager.unwrap_or_else(|| self.0.enqueue(file, offset, length, mode));
            if self.0.fault == AdviceFault::EarlyCompletion {
                return Ok(());
            }
            completion.await.unwrap()
        }
    }
    fn file_advise_submit(
        &self,
        file: &File,
        offset: u64,
        length: u64,
        mode: AdviceMode,
    ) -> impl Future<Output = io::Result<()>> + Send {
        let eager = if self.0.fault == AdviceFault::Eager {
            Some(self.0.enqueue(file, offset, length, mode))
        } else {
            None
        };
        async move {
            if self.0.fault != AdviceFault::IgnoreValidation {
                valid_range(offset, length)?;
            }
            drop(eager.unwrap_or_else(|| self.0.enqueue(file, offset, length, mode)));
            Ok(())
        }
    }
}
struct Hold(Arc<AdviceState>);
impl Drop for Hold {
    fn drop(&mut self) {
        self.0.held.store(false, Ordering::SeqCst);
        self.0.drain();
    }
}
struct AdviceFixture {
    scheduler: Adviser,
    file: File,
    failing: File,
}
impl AdviceFixture {
    fn new(fault: AdviceFault) -> Self {
        Self {
            scheduler: Adviser(Arc::new(AdviceState {
                held: AtomicBool::new(false),
                jobs: Mutex::default(),
                observed: Mutex::default(),
                fault,
            })),
            file: File::new(),
            failing: File {
                failing: true,
                ..File::new()
            },
        }
    }
}
impl FileAdviceFixture for AdviceFixture {
    type Scheduler = Adviser;
    type Guard = Hold;
    fn scheduler(&self) -> &Adviser {
        &self.scheduler
    }
    fn file(&self) -> &File {
        &self.file
    }
    fn failing_file(&self) -> &File {
        &self.failing
    }
    fn error_kind(&self) -> io::ErrorKind {
        io::ErrorKind::Unsupported
    }
    fn assert_state(&self, contents: &[u8], cursor: u64) {
        assert_state(&self.file, contents, cursor);
    }
    fn observed(&self) -> Vec<(u64, u64, AdviceMode)> {
        self.scheduler.0.observed.lock().unwrap().clone()
    }
    fn hold(&self) -> Hold {
        assert!(!self.scheduler.0.held.swap(true, Ordering::SeqCst));
        Hold(Arc::clone(&self.scheduler.0))
    }
    async fn accepted(&self) {
        assert!(!self.scheduler.0.jobs.lock().unwrap().is_empty());
    }
    fn assert_retained(&self) {
        assert!(Arc::strong_count(&self.file.data) > 1);
    }
    async fn settle(&self) {
        self.scheduler.0.drain();
    }
}
fn run_advice(test: impl AsyncFnOnce(&AdviceFixture) + Send + 'static) {
    support::run(move || block_on(test(&AdviceFixture::new(AdviceFault::None))));
}
mod advice {
    base_scheduler_conformance::file_advice_tests!(run: super::run_advice);
}
#[test]
fn rejects_advice_contract_violations() {
    for fault in [
        AdviceFault::Eager,
        AdviceFault::ChangeCursor,
        AdviceFault::WrongRange,
        AdviceFault::EarlyCompletion,
        AdviceFault::AbandonAccepted,
        AdviceFault::IgnoreValidation,
        AdviceFault::LoseError,
    ] {
        support::run(move || {
            let fixture = AdviceFixture::new(fault);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                block_on(async {
                    match fault {
                        AdviceFault::Eager => file_advice::lazy(&fixture).await,
                        AdviceFault::EarlyCompletion | AdviceFault::AbandonAccepted => {
                            file_advice::abandonment(&fixture).await
                        }
                        AdviceFault::IgnoreValidation | AdviceFault::LoseError => {
                            file_advice::errors(&fixture).await
                        }
                        _ => file_advice::modes(&fixture).await,
                    }
                })
            }));
            assert!(
                result.is_err(),
                "advice suite accepted an incorrect backend"
            );
        });
    }
}
