// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{ring_binding::RingBinding, *};
use std::{
    pin::Pin,
    sync::Mutex,
    task::{Context, Wake, Waker},
};

/// One ordered stream of owned writes, retaining at most one unfinished logical write. Ready writes use a direct syscall; unfinished writes advance in their owning ring without an executor task.
///
/// A stalled submission returns after its first SQE has been consumed by the kernel, or after an accepted failure. Ring sends request `MSG_WAITALL`, allowing the kernel to advance partial sends while the caller computes. Completion and buffer retirement remain independent of this submission acknowledgement.
pub(crate) struct IoUringStreamWriter<B: IoBuf> {
    owner: Weak<DriverGroup>,
    socket: RegisteredSocket,
    shared: Arc<Shared<B>>,
    cached: Option<Box<WriteRequest<B>>>,
    running: Option<RingBinding<u64>>,
    error: Option<OperationError>,
    closed: bool,
    failed: bool,
}

struct Shared<B: IoBuf> {
    result: Mutex<Option<Completion<B>>>,
    ready: AtomicBool,
    submitted: AtomicBool,
    waker: atomic_waker::AtomicWaker,
}

enum Completion<B: IoBuf> {
    Retired(OperationResult<()>, Option<Box<WriteRequest<B>>>),
    Failed,
}

impl<B: IoBuf> Shared<B> {
    fn publish(&self, result: Completion<B>) {
        *self.result.lock().unwrap() = Some(result);
        self.ready.store(true, Ordering::Release);
        self.waker.wake();
    }
}

impl<B: IoBuf> submission_notifications::SubmissionNotification for Shared<B> {
    fn submitted(&self) {
        self.submitted.store(true, Ordering::Release);
        self.waker.wake();
    }
}

struct CancelWrite {
    controller: Arc<Controller>,
    identifier: u64,
}

impl Wake for CancelWrite {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.controller.send(Command::Cancel(self.identifier));
    }
}

struct WriteRequest<B: IoBuf> {
    identifier: u64,
    socket: Option<RegisteredSocket>,
    buffer: Option<B>,
    length: usize,
    offset: Cell<usize>,
    polling: Cell<bool>,
    cancelling: Cell<bool>,
    cancellation: Cancellation,
    watch: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    cancel_waker: Option<Arc<CancelWrite>>,
    reply: Cell<Option<Arc<Shared<B>>>>,
    announced: bool,
}

impl<B: IoBuf> WriteRequest<B> {
    fn empty() -> Self {
        Self {
            identifier: 0,
            socket: None,
            buffer: None,
            length: 0,
            offset: Cell::new(0),
            polling: Cell::new(false),
            cancelling: Cell::new(false),
            cancellation: Cancellation::never(),
            watch: None,
            cancel_waker: None,
            reply: Cell::new(None),
            announced: false,
        }
    }

    fn release(&mut self) {
        // Retire memory access and socket ownership before publishing completion, including before returning pooled capacity to a concurrently executing receiver.
        discard(self.buffer.take());
        drop(self.socket.take());
        drop(self.watch.take());
        self.cancellation = Cancellation::never();
    }

    fn watch_cancellation(&mut self, controller: &Arc<Controller>, cancellation: &Cancellation) {
        self.cancellation = cancellation.clone();
        if cancellation.is_never() {
            self.watch = None;
            self.cancel_waker = None;
            return;
        }
        // A delayed notification may retain an old waker after its listener is removed. Reuse storage only with exclusive ownership, preserving the old request ID in every surviving clone.
        if let Some(waker) = self.cancel_waker.as_mut().and_then(Arc::get_mut) {
            waker.controller = Arc::clone(controller);
            waker.identifier = self.identifier;
        } else {
            self.cancel_waker = Some(Arc::new(CancelWrite {
                controller: Arc::clone(controller),
                identifier: self.identifier,
            }));
        }
        let token = cancellation.clone();
        self.watch = Some(Box::pin(async move { token.cancelled().await }));
    }
}

impl<B: IoBuf> PendingRequest for WriteRequest<B> {
    fn identifier(&self) -> u64 {
        self.identifier
    }

    fn prepare(&mut self, _ring: &io_uring::Submitter<'_>) -> io::Result<()> {
        // Pending cancellations retire directly, and cancelling in-flight requests never retry. Preparation therefore needs only the submission token's persistent cancellation state.
        // Register only after Submit has reached the owning driver. A concurrent cancellation can then safely refer to an already published request, including when it crosses the local and remote command queues.
        if let Some(watch) = self.watch.as_mut() {
            let waker = Waker::from(Arc::clone(self.cancel_waker.as_ref().unwrap()));
            let mut context = Context::from_waker(&waker);
            if watch.as_mut().poll(&mut context).is_ready() {
                return Err(io::ErrorKind::Interrupted.into());
            }
        }
        Ok(())
    }

    fn entry(&mut self) -> squeue::Entry {
        let descriptor = types::Fd(self.socket.as_ref().unwrap().as_raw_fd());
        let entry = if self.polling.get() {
            opcode::PollAdd::new(descriptor, libc::POLLOUT as u32).build()
        } else {
            let bytes = &self.buffer.as_ref().unwrap().as_ref()[self.offset.get()..];
            opcode::Send::new(
                descriptor,
                bytes.as_ptr(),
                bytes.len().min(i32::MAX as usize) as u32,
            )
            .flags(libc::MSG_NOSIGNAL | libc::MSG_WAITALL)
            .build()
        };
        entry.user_data(self.identifier)
    }

    fn submission_notification(
        &mut self,
    ) -> Option<Arc<dyn submission_notifications::SubmissionNotification>> {
        if self.announced {
            return None;
        }
        self.announced = true;
        let shared = self.reply.take().unwrap();
        self.reply.set(Some(Arc::clone(&shared)));
        Some(shared)
    }

    fn completion(&self, result: i32, _flags: u32) {
        if self.polling.get() {
            if result >= 0 {
                self.polling.set(false);
            }
        } else if result > 0 {
            self.offset.set(self.offset.get() + result as usize);
        } else if result == -libc::EAGAIN {
            self.polling.set(true);
        }
    }

    fn retry(&self, result: i32) -> Option<bool> {
        if self.offset.get() == self.length
            || self.cancelling.get()
            || self.cancellation.is_cancelled()
        {
            return None;
        }
        (result > 0 || result == -libc::EAGAIN || result == -libc::EINTR).then_some(true)
    }

    fn retire_completion(self: Box<Self>, _ring: &io_uring::Submitter<'_>, result: i32) {
        let result = if self.offset.get() == self.length {
            Ok(self.length)
        } else if result < 0 {
            Err(io::Error::from_raw_os_error(-result))
        } else if result == 0 {
            Err(io::ErrorKind::WriteZero.into())
        } else {
            // Cancellation or shutdown stopped a partially completed logical write.
            Err(io::ErrorKind::Interrupted.into())
        };
        self.finish(result);
    }

    fn finish(mut self: Box<Self>, result: io::Result<usize>) {
        let shared = self.reply.take().unwrap();
        let result = operation_result(result, &self.cancellation).map(|_| ());
        self.release();
        shared.publish(Completion::Retired(result, Some(self)));
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

struct Failure<B: IoBuf>(Arc<Shared<B>>);
impl<B: IoBuf> Drop for Failure<B> {
    fn drop(&mut self) {
        self.0.publish(Completion::Failed);
    }
}

impl<B: IoBuf> Drop for WriteRequest<B> {
    fn drop(&mut self) {
        if let Some(shared) = self.reply.take() {
            // Only unpublished commands are destroyed normally. Unknown kernel access is retained by InFlight instead.
            self.release();
            shared.publish(Completion::Retired(
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "selector closed before accepting the write",
                )
                .into()),
                None,
            ));
        }
    }
}

fn copy_error(error: &OperationError) -> OperationError {
    match error {
        OperationError::Cancelled(cancelled) => (*cancelled).into(),
        OperationError::Io(error) => error
            .raw_os_error()
            .map(io::Error::from_raw_os_error)
            .unwrap_or_else(|| io::Error::new(error.kind(), error.to_string()))
            .into(),
    }
}

impl<B: IoBuf> IoUringStreamWriter<B> {
    fn check(&self, selector: &IoUringSelector) -> io::Result<()> {
        if !std::ptr::eq(self.owner.as_ptr(), Arc::as_ptr(&selector.group)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "writer belongs to a different selector",
            ));
        }
        Ok(())
    }

    fn collect(&mut self) {
        // Only wait calls this method, after observing the published completion with acquire ordering.
        self.running = None;
        let completion = self.shared.result.lock().unwrap().take().unwrap();
        match completion {
            Completion::Retired(result, cached) => {
                self.cached = cached;
                if let Err(error) = result {
                    self.error = Some(error);
                }
            }
            Completion::Failed => {
                self.failed = true;
                self.error = Some(
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "writer retirement could not be established",
                    )
                    .into(),
                );
            }
        }
    }

    async fn wait(&mut self, cancellation: &Cancellation) -> OperationResult<()> {
        if self.running.is_some() {
            let mut cancelled = pin!(cancellation.cancelled());
            poll_fn(|context| {
                self.shared.waker.register(context.waker());
                if self.shared.ready.load(Ordering::Acquire) {
                    return Poll::Ready(Ok(()));
                }
                if cancelled.as_mut().poll(context).is_ready() {
                    return Poll::Ready(Err(OperationError::from(Cancelled)));
                }
                Poll::Pending
            })
            .await?;
            self.collect();
        }
        match &self.error {
            Some(error) => Err(copy_error(error)),
            None => Ok(()),
        }
    }
}

impl IoUringSelector {
    pub(crate) fn socket_stream_output<B: IoBuf>(
        &self,
        socket: &NativeSocket,
    ) -> io::Result<IoUringStreamWriter<B>> {
        self.check_open()?;
        let socket = socket::retain_socket(socket);
        if syscall("stream-socket-type", || socket.get_ref().r#type())? != socket2::Type::STREAM {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ordered writers require a stream socket",
            ));
        }
        Ok(IoUringStreamWriter {
            owner: Arc::downgrade(&self.group),
            socket,
            shared: Arc::new(Shared {
                result: Mutex::new(None),
                ready: AtomicBool::new(false),
                submitted: AtomicBool::new(false),
                waker: atomic_waker::AtomicWaker::new(),
            }),
            cached: None,
            running: None,
            error: None,
            closed: false,
            failed: false,
        })
    }
}

impl<B: IoBuf> IoUringStreamWriter<B> {
    pub(crate) async fn submit(
        &mut self,
        selector: &IoUringSelector,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<(), (OperationError, B)> {
        let checked = self
            .check(selector)
            .map_err(OperationError::from)
            .and_then(|()| cancellation.check().map_err(Into::into))
            .and_then(|()| {
                if self.closed {
                    Err(io::Error::new(io::ErrorKind::BrokenPipe, "writer is closed").into())
                } else {
                    Ok(())
                }
            });
        if let Err(error) = checked {
            return Err((error, buffer));
        }
        if let Err(error) = self.wait(cancellation).await {
            return Err((error, buffer));
        }
        if let Err(error) = selector.check_open() {
            return Err((error.into(), buffer));
        }
        if let Err(error) = cancellation.check() {
            return Err((error.into(), buffer));
        }
        // Acceptance happens before the first syscall. Every later error belongs to this logical write and is reported by flush or the next submit.
        let length = buffer.as_ref().len();
        if length == 0 {
            return Ok(());
        }
        let result = {
            let send = || {
                syscall("stream-write", || {
                    self.socket.get_ref().send_with_flags(
                        &buffer.as_ref()[..length.min(i32::MAX as usize)],
                        libc::MSG_NOSIGNAL | libc::MSG_DONTWAIT,
                    )
                })
            };
            #[cfg(test)]
            {
                tests::send(send)
            }
            #[cfg(not(test))]
            {
                send()
            }
        };
        let offset = match result {
            Ok(count) if count == length => return Ok(()),
            Ok(0) => {
                self.error = Some(io::Error::from(io::ErrorKind::WriteZero).into());
                return Ok(());
            }
            Ok(count) => count,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                0
            }
            Err(error) => {
                self.error = Some(error.into());
                return Ok(());
            }
        };
        let controller = selector.route();
        let identifier = match controller.next_identifier() {
            Ok(identifier) => identifier,
            Err(error) => {
                self.error = Some(error.into());
                return Ok(());
            }
        };
        let mut request = self
            .cached
            .take()
            .unwrap_or_else(|| Box::new(WriteRequest::empty()));
        request.identifier = identifier;
        request.socket = Some(Arc::clone(&self.socket));
        request.buffer = Some(buffer);
        request.length = length;
        request.offset.set(offset);
        request.polling.set(false);
        request.cancelling.set(false);
        request.watch_cancellation(&controller, cancellation);
        request.reply.set(Some(Arc::clone(&self.shared)));
        request.announced = false;
        self.shared.ready.store(false, Ordering::Relaxed);
        self.shared.submitted.store(false, Ordering::Relaxed);
        self.running = Some(RingBinding::new(&controller, identifier));
        // The persistent writer bounds itself to one active logical write, just as a persistent readiness monitor bounds itself to one waiter. It does not occupy the transient-operation admission pool.
        controller.submit(request);
        // A complete immediate send returns above. An asynchronous send returns only after the driver observes the kernel consume its SQE, or the accepted write ends before that acknowledgement. This crosses a real loop boundary even if the caller is stolen to another worker.
        poll_fn(|context| {
            self.shared.waker.register(context.waker());
            if self.shared.submitted.load(Ordering::Acquire)
                || self.shared.ready.load(Ordering::Acquire)
            {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        Ok(())
    }

    pub(crate) async fn flush(
        &mut self,
        selector: &IoUringSelector,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check(selector)?;
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "writer is closed").into());
        }
        self.wait(cancellation).await
    }

    pub(crate) async fn close(&mut self, selector: &IoUringSelector) -> io::Result<()> {
        self.check(selector)?;
        self.closed = true;
        if let Some(running) = &self.running {
            if !self.shared.ready.load(Ordering::Acquire) {
                let _ = running.controller.send(Command::Cancel(running.state));
            }
            let _ = self.wait(&Cancellation::never()).await;
        }
        self.cached = None;
        if self.failed {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "writer retirement could not be established",
            ))
        } else {
            Ok(())
        }
    }
}

impl<B: IoBuf> Drop for IoUringStreamWriter<B> {
    fn drop(&mut self) {
        if let Some(running) = &self.running {
            if !self.shared.ready.load(Ordering::Acquire) {
                let _ = running.controller.send(Command::Cancel(running.state));
            }
        }
    }
}

#[cfg(test)]
mod tests;
