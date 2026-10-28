// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use super::{
    registered_file::RegisteredFile,
    ring_binding::{RingBinding, RingCache},
};
use std::{any::Any, sync::Mutex};

/// Reusable file request storage associated with one io_uring selector group. The request can select a different worker between completed operations.
pub(crate) struct IoUringFileRegistration {
    owner: Weak<DriverGroup>,
    shared: Arc<Shared>,
    cached: Option<Box<dyn Any + Send>>,
    running: Option<RingBinding<u64>>,
    file: Arc<StdFile>,
    files: RingCache<Arc<RegisteredFile>>,
    retiring: Vec<Arc<RegisteredFile>>,
    closed: bool,
    failed: bool,
}

enum Completion {
    Cached(Box<dyn Any + Send>),
    Request(io::Result<usize>, Box<dyn Any + Send>),
    Rejected(Box<dyn Any + Send>),
    Failed,
}

struct Shared {
    result: Mutex<Option<Completion>>,
    ready: AtomicBool,
    failed: AtomicBool,
    collecting: AtomicBool,
    waker: atomic_waker::AtomicWaker,
    retirement_waker: atomic_waker::AtomicWaker,
}

impl Shared {
    fn publish(&self, result: Completion) {
        if matches!(&result, Completion::Failed) {
            self.failed.store(true, Ordering::Release);
        }
        let abandoned = {
            let mut slot = self.result.lock().unwrap();
            if self.collecting.load(Ordering::Acquire) {
                *slot = Some(result);
                None
            } else {
                Some(result)
            }
        };
        // Abandonment and publication serialize through the result lock, so either side releases a retired owner without waiting for lane reuse.
        discard(abandoned);
        self.ready.store(true, Ordering::Release);
        self.waker.wake();
        self.retirement_waker.wake();
    }

    fn take(&self) -> Option<Completion> {
        self.result.lock().unwrap().take()
    }
}

// Abandoned owners may run arbitrary destructors. Their disposal must not unwind the worker publishing the last retirement.
impl Drop for Shared {
    fn drop(&mut self) {
        discard(
            self.result
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
    }
}

// Collection can outlive its lane. Close retires backend access without consuming a live completion's owner; abandonment releases this claim and requests cancellation.
struct CompletionWait {
    cancel: CancelOnDrop,
    shared: Arc<Shared>,
}
impl CompletionWait {
    fn disarm(&mut self) {
        self.cancel.armed = false;
    }
}
impl Drop for CompletionWait {
    fn drop(&mut self) {
        let abandoned = {
            let mut result = self.shared.result.lock().unwrap();
            self.shared.collecting.store(false, Ordering::Release);
            match result.as_ref() {
                Some(Completion::Cached(_)) => None,
                _ => result.take(),
            }
        };
        discard(abandoned);
    }
}

struct FileRequest<B: IoBuf> {
    identifier: u64,
    permit: Option<Permit>,
    file: Option<Arc<RegisteredFile>>,
    offset: Option<u64>,
    buffer: Option<B>,
    pointer: fn(&mut B) -> *mut u8,
    write: bool,
    reply: Cell<Option<Arc<Shared>>>,
    cancelling: Cell<bool>,
}

impl<B: IoBuf> PendingRequest for FileRequest<B> {
    fn identifier(&self) -> u64 {
        self.identifier
    }

    fn prepare(&mut self, ring: &io_uring::Submitter<'_>) -> io::Result<()> {
        self.file.as_ref().unwrap().prepare(ring);
        Ok(())
    }

    fn entry(&mut self) -> squeue::Entry {
        let buffer = self.buffer.as_mut().unwrap();
        let length = buffer.as_ref().len().min(i32::MAX as usize) as u32;
        let pointer = (self.pointer)(buffer);
        let file = self.file.as_ref().unwrap();
        let descriptor = types::Fd(file.file.as_raw_fd());
        let fixed = file.index();
        let offset = self.offset.unwrap_or(u64::MAX);
        let entry = if self.write {
            match fixed {
                Some(fixed) => opcode::Write::new(fixed, pointer, length)
                    .offset(offset)
                    .build(),
                None => opcode::Write::new(descriptor, pointer, length)
                    .offset(offset)
                    .build(),
            }
        } else {
            match fixed {
                Some(fixed) => opcode::Read::new(fixed, pointer, length)
                    .offset(offset)
                    .build(),
                None => opcode::Read::new(descriptor, pointer, length)
                    .offset(offset)
                    .build(),
            }
        };
        entry.user_data(self.identifier)
    }

    fn finish(mut self: Box<Self>, result: io::Result<usize>) {
        let shared = self.reply.take().unwrap();
        // Release kernel-accessible resources and capacity before publishing retirement. The cached box has no back-reference to Shared.
        drop(self.file.take());
        drop(self.permit.take());
        shared.publish(Completion::Request(result, self));
    }

    fn begin_cancellation(&self) -> bool {
        !self.cancelling.replace(true)
    }

    fn take_reply(&self) -> Option<Box<dyn Send>> {
        self.reply
            .take()
            .map(|shared| Box::new(Failure(shared)) as Box<dyn Send>)
    }
}

impl<B: IoBuf> Drop for FileRequest<B> {
    fn drop(&mut self) {
        drop(self.file.take());
        drop(self.permit.take());
        // Queued commands may be destroyed before reaching the ring. Published requests with unknown retirement are deliberately leaked by InFlight instead.
        if let Some(shared) = self.reply.take() {
            shared.publish(Completion::Rejected(Box::new(self.buffer.take().unwrap())));
        }
    }
}

struct Failure(Arc<Shared>);
impl Drop for Failure {
    fn drop(&mut self) {
        self.0.publish(Completion::Failed);
    }
}

impl IoUringSelector {
    fn check_file_monitor(&self, monitor: &IoUringFileRegistration) -> io::Result<()> {
        if !std::ptr::eq(monitor.owner.as_ptr(), Arc::as_ptr(&self.group)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file monitor belongs to a different selector",
            ));
        }
        Ok(())
    }

    pub(crate) async fn file_monitor_operation<B: IoBuf>(
        &self,
        monitor: &mut IoUringFileRegistration,
        buffer: B,
        offset: Option<u64>,
        write: bool,
        pointer: fn(&mut B) -> *mut u8,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        match self
            .file_monitor_operation_submit(monitor, buffer, offset, write, pointer, cancellation)
            .await
        {
            Ok(completion) => completion.await,
            Err((error, buffer)) => (Err(error), buffer),
        }
    }

    pub(crate) async fn file_monitor_operation_submit<B: IoBuf>(
        &self,
        monitor: &mut IoUringFileRegistration,
        buffer: B,
        offset: Option<u64>,
        write: bool,
        pointer: fn(&mut B) -> *mut u8,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        let check = cancellation
            .check()
            .map_err(OperationError::from)
            .and_then(|()| self.check_file_monitor(monitor).map_err(Into::into));
        if let Err(error) = check {
            return Err((error, buffer));
        }
        if offset.is_some_and(|offset| offset > i64::MAX as u64) {
            return Err((
                io::Error::new(io::ErrorKind::InvalidInput, "file offset exceeds i64::MAX").into(),
                buffer,
            ));
        }
        let registration = &mut *monitor;
        if registration.closed || registration.shared.failed.load(Ordering::Acquire) {
            return Err((
                io::Error::new(io::ErrorKind::BrokenPipe, "file monitor is closed").into(),
                buffer,
            ));
        }
        if registration.running.is_some() {
            if !registration.shared.ready.load(Ordering::Acquire) {
                return Err((
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "file monitor operation has not retired",
                    )
                    .into(),
                    buffer,
                ));
            }
            if registration.shared.collecting.load(Ordering::Acquire) {
                return Err((
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "file operation result has not been collected",
                    )
                    .into(),
                    buffer,
                ));
            }
            registration.running = None;
            // Abandonment clears an uncollected owner under the result lock. Only a successfully collected request leaves reusable storage.
            if let Some(Completion::Cached(cached)) = registration.shared.take() {
                registration.cached = Some(cached);
            }
        }
        if let Err(error) = self.check_open() {
            return Err((error.into(), buffer));
        }
        let shared = Arc::clone(&registration.shared);
        let controller = self.route();
        let permit = match controller.capacity.acquire(cancellation).await {
            Ok(permit) => permit,
            Err(error) => {
                return Err((
                    operation_result::<usize>(Err(error), cancellation).unwrap_err(),
                    buffer,
                ));
            }
        };
        let identifier = match controller.next_identifier() {
            Ok(identifier) => identifier,
            Err(error) => return Err((error.into(), buffer)),
        };
        let cached = monitor.cached.take();
        let mut request = cached
            .and_then(|cached| cached.downcast::<FileRequest<B>>().ok())
            .unwrap_or_else(|| {
                Box::new(FileRequest {
                    identifier,
                    permit: None,
                    file: None,
                    offset,
                    buffer: None,
                    pointer,
                    write,
                    reply: Cell::new(None),
                    cancelling: Cell::new(false),
                })
            });
        drop(request.buffer.take());
        request.identifier = identifier;
        request.permit = Some(permit);
        let registration = &mut *monitor;
        // Keep only the current registration; unfinished or failed releases remain observable until close.
        registration.retiring.retain(|file| !file.released());
        if !registration.files.matches(&controller) {
            if let Some(previous) = registration.files.take_current() {
                previous.retire();
                registration.retiring.push(previous);
            }
        }
        let file = &registration.file;
        request.file = Some(Arc::clone(
            registration
                .files
                .get_or_insert(&controller, || RegisteredFile::new(&controller, file)),
        ));
        request.offset = offset;
        request.buffer = Some(buffer);
        request.pointer = pointer;
        request.write = write;
        request.reply.set(Some(Arc::clone(&shared)));
        request.cancelling.set(false);
        shared.ready.store(false, Ordering::Relaxed);
        monitor.running = Some(RingBinding::new(&controller, identifier));
        shared.collecting.store(true, Ordering::Release);
        if let Err(mpsc::SendError(Command::Submit(request))) =
            controller.send(Command::Submit(request))
        {
            request.finish(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring selector has exited",
            )));
        }
        let cancel = CancelOnDrop {
            controller,
            identifier,
            armed: true,
        };
        let cancellation = cancellation.clone();
        let mut completion = CompletionWait {
            cancel,
            shared: Arc::clone(&shared),
        };
        Ok(async move {
            let mut cancelled = pin!(cancellation.cancelled());
            poll_fn(|context| {
                shared.waker.register(context.waker());
                if shared.ready.load(Ordering::Acquire) {
                    return Poll::Ready(());
                }
                if completion.cancel.armed && cancelled.as_mut().poll(context).is_ready() {
                    completion.cancel.request();
                }
                Poll::Pending
            })
            .await;
            completion.disarm();
            let mut result_slot = shared.result.lock().unwrap();
            match result_slot
                .take()
                .expect("accepted file work publishes one completion")
            {
                Completion::Request(result, cached) => {
                    let mut request = cached.downcast::<FileRequest<B>>().ok().unwrap();
                    let buffer = request.buffer.take().unwrap();
                    *result_slot = Some(Completion::Cached(request));
                    (operation_result(result, &cancellation), buffer)
                }
                Completion::Rejected(buffer) => (
                    Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "selector closed before submission",
                    )
                    .into()),
                    *buffer.downcast::<B>().ok().unwrap(),
                ),
                Completion::Failed => {
                    shared.failed.store(true, Ordering::Release);
                    drop(result_slot);
                    panic!("io_uring selector failed with an operation in flight");
                }
                Completion::Cached(_) => {
                    unreachable!("each accepted file operation has only one completion future")
                }
            }
        })
    }
}

impl IoUringSelector {
    pub(crate) async fn file_monitor_read_submit<B: IoBufMut>(
        &self,
        monitor: &mut IoUringFileRegistration,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.file_monitor_operation_submit(
            monitor,
            buffer,
            None,
            false,
            |buffer| buffer.as_mut().as_mut_ptr(),
            cancellation,
        )
        .await
    }

    pub(crate) async fn file_monitor_write_submit<B: IoBuf>(
        &self,
        monitor: &mut IoUringFileRegistration,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.file_monitor_operation_submit(
            monitor,
            buffer,
            None,
            true,
            |buffer| buffer.as_ref().as_ptr().cast_mut(),
            cancellation,
        )
        .await
    }

    pub(crate) async fn file_monitor_read_at_submit<B: IoBufMut>(
        &self,
        monitor: &mut IoUringFileRegistration,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.file_monitor_operation_submit(
            monitor,
            buffer,
            Some(offset),
            false,
            |buffer| buffer.as_mut().as_mut_ptr(),
            cancellation,
        )
        .await
    }

    pub(crate) async fn file_monitor_write_at_submit<B: IoBuf>(
        &self,
        monitor: &mut IoUringFileRegistration,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.file_monitor_operation_submit(
            monitor,
            buffer,
            Some(offset),
            true,
            |buffer| buffer.as_ref().as_ptr().cast_mut(),
            cancellation,
        )
        .await
    }
}

impl IoUringSelector {
    pub(crate) fn file_monitor(&self, file: &NativeFile) -> io::Result<IoUringFileRegistration> {
        self.check_open()?;
        Ok(IoUringFileRegistration {
            owner: Arc::downgrade(&self.group),
            cached: None,
            running: None,
            file: retain_file(file),
            files: RingCache::default(),
            retiring: Vec::new(),
            closed: false,
            failed: false,
            shared: Arc::new(Shared {
                result: Mutex::new(None),
                ready: AtomicBool::new(false),
                failed: AtomicBool::new(false),
                collecting: AtomicBool::new(false),
                waker: atomic_waker::AtomicWaker::new(),
                retirement_waker: atomic_waker::AtomicWaker::new(),
            }),
        })
    }

    pub(crate) async fn file_monitor_read<B: IoBufMut>(
        &self,
        monitor: &mut IoUringFileRegistration,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.file_monitor_operation(
            monitor,
            buffer,
            None,
            false,
            |buffer| buffer.as_mut().as_mut_ptr(),
            cancellation,
        )
        .await
    }
    pub(crate) async fn file_monitor_write<B: IoBuf>(
        &self,
        monitor: &mut IoUringFileRegistration,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.file_monitor_operation(
            monitor,
            buffer,
            None,
            true,
            |buffer| buffer.as_ref().as_ptr().cast_mut(),
            cancellation,
        )
        .await
    }
    pub(crate) async fn file_monitor_read_at<B: IoBufMut>(
        &self,
        monitor: &mut IoUringFileRegistration,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.file_monitor_operation(
            monitor,
            buffer,
            Some(offset),
            false,
            |buffer| buffer.as_mut().as_mut_ptr(),
            cancellation,
        )
        .await
    }
    pub(crate) async fn file_monitor_write_at<B: IoBuf>(
        &self,
        monitor: &mut IoUringFileRegistration,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.file_monitor_operation(
            monitor,
            buffer,
            Some(offset),
            true,
            |buffer| buffer.as_ref().as_ptr().cast_mut(),
            cancellation,
        )
        .await
    }

    pub(crate) async fn file_monitor_close(
        &self,
        monitor: &mut IoUringFileRegistration,
    ) -> io::Result<()> {
        self.check_file_monitor(monitor)?;
        let registration = &mut *monitor;
        registration.closed = true;
        if registration.failed || registration.shared.failed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "selector failed before file retirement",
            ));
        }
        if let Some(binding) = &registration.running {
            if !registration.shared.ready.load(Ordering::Acquire)
                && !registration.shared.collecting.load(Ordering::Acquire)
            {
                let _ = binding.controller.send(Command::Cancel(binding.state));
            }
            poll_fn(|context| {
                registration
                    .shared
                    .retirement_waker
                    .register(context.waker());
                if registration.shared.ready.load(Ordering::Acquire) {
                    return Poll::Ready(());
                }
                Poll::Pending
            })
            .await;
            registration.running = None;
            if registration.shared.failed.load(Ordering::Acquire) {
                registration.failed = true;
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "selector failed before file retirement",
                ));
            }
            if !registration.shared.collecting.load(Ordering::Acquire) {
                drop(registration.shared.take());
            }
        }
        drop(registration.cached.take());
        for file in registration.files.values() {
            file.retire();
        }
        for file in registration.files.values() {
            file.closed().await?;
        }
        for file in &registration.retiring {
            file.closed().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
