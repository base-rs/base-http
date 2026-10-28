// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Persistent receives. Only the ring owner publishes or recycles provided buffers.

use super::*;
use std::{cell::UnsafeCell, sync::Mutex, task::Context};

pub(crate) mod pool;
use pool::Pool;

use crate::RecvMsgMultishotOptions;
use crate::io_uring_recv_message::{IoUringRecvMessage, MessageStorage};

/// Counters for verifying persistent submissions and pool pressure.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecvMsgMultishotStatistics {
    pub submissions: u64,
    pub messages: u64,
    pub buffer_exhaustions: u64,
}

/// An immutable lease of a completed receive buffer. Drop returns it to its original ring.
pub struct RecvMsgBuffer {
    pool: Arc<Pool>,
    identifier: u16,
    offset: usize,
    length: usize,
}
impl AsRef<[u8]> for RecvMsgBuffer {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: The CQE transferred this buffer out of the provided ring. It is not published again until this unique lease is dropped.
        unsafe { self.pool.bytes(self.identifier, self.offset, self.length) }
    }
}
impl MessageStorage for RecvMsgBuffer {
    fn storage(&self) -> &[u8] {
        // SAFETY: The selected buffer remains exclusively retained through the initialized prefix containing its metadata and completed payload.
        unsafe {
            self.pool
                .bytes(self.identifier, 0, self.offset + self.length)
        }
    }
}
// SAFETY: The lease exclusively retains this completed, initialized range at a stable allocation. Moving it cannot change its pointer or length.
unsafe impl IoBuf for RecvMsgBuffer {}
impl Drop for RecvMsgBuffer {
    fn drop(&mut self) {
        if crate::io_uring_worker::recycle(&self.pool, self.identifier) {
            return;
        }
        if let Some(controller) = self.pool.controller.upgrade() {
            let _ =
                controller.send_remote(Command::Recycle(Arc::clone(&self.pool), self.identifier));
        }
    }
}

pub(super) struct Received<B: IoBuf = RecvMsgBuffer> {
    pub(super) messages: VecDeque<IoUringRecvMessage<B>>,
    retired: bool,
    error: Option<io::Error>,
    pub(super) statistics: RecvMsgMultishotStatistics,
}
impl<B: IoBuf> Default for Received<B> {
    fn default() -> Self {
        Self {
            messages: VecDeque::new(),
            retired: false,
            error: None,
            statistics: Default::default(),
        }
    }
}
pub(super) struct State<B: IoBuf = RecvMsgBuffer> {
    pub(super) received: Mutex<Received<B>>,
    pub(super) changed: CompletionEvent,
}
impl<B: IoBuf> Default for State<B> {
    fn default() -> Self {
        Self {
            received: Mutex::new(Received::default()),
            changed: CompletionEvent::new(),
        }
    }
}
impl<B: IoBuf> State<B> {
    fn next(&self) -> Option<OperationResult<Option<IoUringRecvMessage<B>>>> {
        let mut received = self.received.lock().unwrap();
        if let Some(message) = received.messages.pop_front() {
            Some(Ok(Some(message)))
        } else if received.retired {
            Some(
                received
                    .error
                    .take()
                    .map_or(Ok(None), |error| Err(error.into())),
            )
        } else {
            None
        }
    }

    pub(super) fn finish(&self, result: io::Result<usize>) {
        {
            let mut received = self.received.lock().unwrap();
            received.retired = true;
            received.error = result.err();
        }
        self.changed.notify(usize::MAX);
    }
}

/// A persistent receive on its original ring, even if the consumer moves threads.
///
/// `recv` returns completed messages without resubmitting the receive. Retaining all buffers pauses reception; returning a buffer rearms an exhausted request. TCP byte ordering and datagram boundaries are preserved. Do not run other receives concurrently on this socket unless your protocol permits competing readers. Drop requests cancellation; `close` waits for terminal original completion.
pub struct RecvMsgMultishot<B: IoBuf = RecvMsgBuffer> {
    pub(super) state: Arc<State<B>>,
    pub(super) cancel: CancelOnDrop,
}
impl<B: IoBuf> RecvMsgMultishot<B> {
    /// Receive the next message; `None` is terminal stream EOF. Empty datagrams are messages. Successful queued progress wins concurrent cancellation. Cancellation with no queued message closes and drains the receiver.
    async fn receive(
        &mut self,
        cancellation: &Cancellation,
    ) -> OperationResult<Option<IoUringRecvMessage<B>>> {
        let mut cancelled = pin!(cancellation.cancelled());
        loop {
            // Completed batches need no listener registration. Listen and recheck only when the queue is empty, closing the race with completion publication.
            if let Some(result) = self.state.next() {
                return result;
            }
            #[cfg(test)]
            tests::before_receive_listen();
            let was_cancelled = {
                event_listener::listener!(self.state.changed => listener);
                if let Some(result) = self.state.next() {
                    return result;
                }
                let mut listener = pin!(listener);
                poll_fn(|context: &mut Context<'_>| {
                    if listener.as_mut().poll(context).is_ready() {
                        Poll::Ready(false)
                    } else {
                        cancelled.as_mut().poll(context).map(|_| true)
                    }
                })
                .await
            };
            if was_cancelled {
                self.retire().await?;
                return Err(Cancelled.into());
            }
        }
    }

    pub fn statistics(&self) -> RecvMsgMultishotStatistics {
        self.state.received.lock().unwrap().statistics
    }

    /// Stop receiving, await original-request retirement, and discard unread messages. Already returned buffer leases remain valid, including after scheduler shutdown.
    async fn retire(&mut self) -> io::Result<()> {
        self.cancel.request();
        loop {
            event_listener::listener!(self.state.changed => listener);
            let retired = {
                let mut received = self.state.received.lock().unwrap();
                if received.retired {
                    let messages = std::mem::take(&mut received.messages);
                    let error = received.error.take();
                    Some((messages, error))
                } else {
                    None
                }
            };
            if let Some((messages, error)) = retired {
                // Recycling can wake the owner; never drop leases while holding the result lock.
                drop(messages);
                return match error {
                    Some(error)
                        if error.raw_os_error() != Some(libc::ECANCELED)
                            && error.kind() != io::ErrorKind::Interrupted =>
                    {
                        Err(error)
                    }
                    _ => Ok(()),
                };
            }
            listener.await;
        }
    }
}

impl<B: IoBuf> RecvMsgMultishot<B> {
    /// Receive from this bound sequence. The original ring remains its owner after worker migration.
    pub async fn recv(
        &mut self,
        cancellation: &Cancellation,
    ) -> OperationResult<Option<IoUringRecvMessage<B>>> {
        cancellation.check()?;
        self.receive(cancellation).await
    }
    /// Stop reception and await retirement of every original request.
    pub async fn close(&mut self) -> io::Result<()> {
        self.retire().await
    }
}

impl IoUringSelector {
    /// Start a concrete bounded multishot `recvmsg` receive (Linux 6.0+, provided rings 5.19+). Portable consumers use bound stream or packet readers with a scheduler-created pool.
    ///
    /// Pre-cancellation leaves an existing receiver unchanged. Cancellation of a pending receive stops and retires this concrete receive sequence before returning; delivered immutable leases remain valid. This stronger behavior is specific to this constructor.
    ///
    /// This lazy operation selects one ring and retains the socket through final completion. The returned receiver is a backend resource, not an implicit scheduler. Pool registration errors are reported by its first `recv`. Each receiver holds one ring admission slot until EOF or close; reserve additional capacity for sends and other operations which may need the ring. Peeking is rejected: a persistent peek can repeatedly consume the whole pool with copies of the same datagram. Unsupported kernels fail explicitly.
    pub async fn socket_recvmsg_multishot(
        &self,
        socket: &NativeSocket,
        options: RecvMsgMultishotOptions,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<RecvMsgMultishot> {
        cancellation.check()?;
        self.check_open()?;
        if flags.peek {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistent receives do not support peeking",
            )
            .into());
        }
        let controller = self.route();
        let permit = operation_result(
            controller.capacity.acquire(cancellation).await,
            cancellation,
        )?;
        let identifier = controller.next_identifier()?;
        let pool = Pool::new(&controller, options, identifier)?;
        let socket = super::socket::retain_socket(socket);
        let stream = syscall("socket-type", || socket.get_ref().r#type())? == socket2::Type::STREAM;
        let state = Arc::new(State {
            received: Mutex::new(Received {
                messages: VecDeque::with_capacity(options.buffers as usize),
                ..Default::default()
            }),
            ..Default::default()
        });
        // SAFETY: A zeroed msghdr with only name/control capacities is the multishot ABI. The immutable header stays in its boxed request until final completion.
        let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
        header.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
        let request = MultiRequest {
            identifier,
            _permit: Some(permit),
            socket: Some(socket),
            stream,
            pool,
            state: Arc::clone(&state),
            header: UnsafeCell::new(header),
            cancelling: Cell::new(false),
            registered: Cell::new(false),
            eof: Cell::new(false),
            finished: false,
        };
        controller.submit(Box::new(request));
        Ok(RecvMsgMultishot {
            state,
            cancel: CancelOnDrop {
                controller,
                identifier,
                armed: true,
            },
        })
    }
}

struct MultiRequest {
    identifier: u64,
    _permit: Option<Permit>,
    socket: Option<RegisteredSocket>,
    stream: bool,
    pool: Arc<Pool>,
    state: Arc<State>,
    header: UnsafeCell<libc::msghdr>,
    cancelling: Cell<bool>,
    registered: Cell<bool>,
    eof: Cell<bool>,
    finished: bool,
}
// SAFETY: The boxed request is movable before publication, then exclusively driven by one ring owner. Header and provided allocations remain stable until terminal original completion.
unsafe impl Send for MultiRequest {}

impl PendingRequest for MultiRequest {
    fn identifier(&self) -> u64 {
        self.identifier
    }
    fn prepare(&mut self, ring: &io_uring::Submitter<'_>) -> io::Result<()> {
        if !self.registered.get() {
            self.pool.register(ring)?;
            self.registered.set(true);
        }
        self.pool.set_rearm(false);
        self.state.received.lock().unwrap().statistics.submissions += 1;
        Ok(())
    }
    fn entry(&mut self) -> squeue::Entry {
        opcode::RecvMsgMulti::new(
            types::Fd(
                self.socket
                    .as_ref()
                    .expect("receive retains its socket")
                    .get_ref()
                    .as_raw_fd(),
            ),
            self.header.get(),
            self.pool.group(),
        )
        .build()
        .user_data(self.identifier)
    }
    fn entry_may_wake(&self) -> bool {
        false
    }
    fn completion(&self, result: i32, flags: u32) {
        self.pool.set_rearm(!io_uring::cqueue::more(flags));
        if result == -libc::ENOBUFS {
            self.state
                .received
                .lock()
                .unwrap()
                .statistics
                .buffer_exhaustions += 1;
        }
        let Some(identifier) = io_uring::cqueue::buffer_select(flags) else {
            if result < 0 {
                return;
            }
            // An EOF may have no selected buffer. Other nonnegative results without one violate the multishot receive ABI.
            assert_eq!(
                result, 0,
                "multishot receive data requires a selected buffer"
            );
            self.eof.set(true);
            return;
        };
        self.pool.selected();
        if result <= 0 {
            // Flags transfer buffer ownership independently of the result. Error and zero-byte CQEs have no recvmsg header to inspect.
            self.pool.recycle(identifier);
            if result == 0 {
                self.eof.set(true);
            }
            return;
        }
        // SAFETY: This CQE transfers exclusive access to its selected, initialized buffer. No publication of that buffer can happen until its lease is dropped.
        let bytes = unsafe { self.pool.bytes(identifier, 0, result as usize) };
        // SAFETY: The header is immutable for the entire multishot operation.
        let header = unsafe { &*self.header.get() };
        let parsed = types::RecvMsgOut::parse(bytes, header)
            .expect("kernel returns the recvmsg multishot layout");
        let payload = parsed.payload_data();
        let offset = payload.as_ptr() as usize - bytes.as_ptr() as usize;
        let buffer = RecvMsgBuffer {
            pool: Arc::clone(&self.pool),
            identifier,
            offset,
            length: payload.len(),
        };
        if self.stream && payload.is_empty() {
            self.eof.set(true);
            drop(buffer);
            return;
        }
        let name = parsed.name_data();
        let name_offset = name.as_ptr() as usize - bytes.as_ptr() as usize;
        let message = IoUringRecvMessage::native(
            buffer,
            name_offset..name_offset + name.len(),
            parsed.is_payload_truncated(),
            parsed.is_control_data_truncated(),
        );
        let notify = {
            let mut received = self.state.received.lock().unwrap();
            let empty = received.messages.is_empty();
            received.statistics.messages += 1;
            received.messages.push_back(message);
            empty
        };
        if notify {
            self.state.changed.notify(1);
        }
    }
    fn retry(&self, result: i32) -> Option<bool> {
        (!self.cancelling.get() && !self.eof.get() && (result >= 0 || result == -libc::ENOBUFS))
            .then(|| self.pool.available() != 0)
    }
    fn retire(self: Box<Self>, ring: &io_uring::Submitter<'_>, result: io::Result<usize>) {
        let result = if self.registered.replace(false) {
            match self.pool.unregister(ring) {
                Ok(()) => result,
                Err(error) => {
                    // The kernel may retain the registered ring even after receive retirement. Retain its allocation if unregister fails.
                    std::mem::forget(Arc::clone(&self.pool));
                    Err(error)
                }
            }
        } else {
            result
        };
        self.finish(result);
    }
    fn retire_completion(self: Box<Self>, ring: &io_uring::Submitter<'_>, result: i32) {
        self.retire(ring, retirement(result));
    }
    fn finish(mut self: Box<Self>, result: io::Result<usize>) {
        let state = Arc::clone(&self.state);
        self.finished = true;
        drop(self);
        state.finish(result);
    }
    fn begin_cancellation(&self) -> bool {
        !self.cancelling.replace(true)
    }
    fn take_reply(&self) -> Option<Box<dyn Send>> {
        struct Failed(Arc<State>);
        impl Drop for Failed {
            fn drop(&mut self) {
                self.0.finish(Err(io::Error::other(
                    "io_uring driver failed during multishot receive",
                )));
            }
        }
        Some(Box::new(Failed(Arc::clone(&self.state))))
    }
}

/// Exhaustion normally rearms the receive. If close or shutdown prevents rearming, its terminal CQE still represents successful retirement, not a transport failure. Apply before cleanup so registration failures remain observable.
pub(super) fn retirement(result: i32) -> io::Result<usize> {
    match result {
        result if result == -libc::ENOBUFS => Ok(0),
        result if result < 0 => Err(io::Error::from_raw_os_error(-result)),
        result => Ok(result as usize),
    }
}

impl Drop for MultiRequest {
    fn drop(&mut self) {
        if !self.finished {
            if self.registered.get() {
                std::mem::forget(Arc::clone(&self.pool));
            }
            drop(self.socket.take());
            drop(self._permit.take());
            self.state.finish(Err(io::ErrorKind::BrokenPipe.into()));
        }
    }
}

#[cfg(test)]
mod tests;
