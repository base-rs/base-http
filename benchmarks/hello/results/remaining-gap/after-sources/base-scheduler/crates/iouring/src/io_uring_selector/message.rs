// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::socket::{retain_socket, retry_operation};
use super::*;
use crate::{Interest, SocketMessage};
use std::cell::UnsafeCell;

type MessageResult<B> = (io::Result<SocketRecvResult>, B);
type Prepare<B> = fn(&mut B, &mut Vec<libc::iovec>) -> (*mut libc::iovec, usize);

#[derive(Clone, Copy)]
enum MessageOperation {
    Send(SocketSendFlags, Option<SocketAddress>),
    Receive(SocketRecvFlags),
}

// The request is boxed before entry() derives any pointers. It stays at that address until the original CQE. UnsafeCell permits kernel-written metadata and inline prepared vectors to coexist with the driver's shared bookkeeping references; exclusive access resumes only after retirement.
struct MessageRequest<B: IoBufSequence> {
    identifier: u64,
    _permit: Permit,
    socket: Option<RegisteredSocket>,
    buffers: Option<UnsafeCell<B>>,
    vectors: Vec<libc::iovec>,
    header: UnsafeCell<libc::msghdr>,
    address: UnsafeCell<libc::sockaddr_storage>,
    prepare: Prepare<B>,
    operation: MessageOperation,
    reply: Cell<Option<oneshot::Sender<MessageResult<B>>>>,
    cancelling: Cell<bool>,
}

// SAFETY: Before submission the whole request may move between threads. After submission only its owning driver accesses bookkeeping, and its box, buffers, vectors, and native metadata remain alive and immobile until original completion. No payload or metadata is accessed while the kernel may use it.
unsafe impl<B: IoBufSequence> Send for MessageRequest<B> {}

impl<B: IoBufSequence> PendingRequest for MessageRequest<B> {
    fn identifier(&self) -> u64 {
        self.identifier
    }

    fn entry(&mut self) -> squeue::Entry {
        let descriptor = types::Fd(
            self.socket
                .as_ref()
                .expect("request retains its socket")
                .get_ref()
                .as_raw_fd(),
        );
        let buffers = self
            .buffers
            .as_mut()
            .expect("request retains its buffers")
            .get_mut();
        let (vectors, count) = (self.prepare)(buffers, &mut self.vectors);
        let header = self.header.get_mut();
        header.msg_iov = vectors;
        header.msg_iovlen = count;
        let address = self.address.get();
        match self.operation {
            MessageOperation::Send(flags, destination) => {
                if let Some(destination) = destination {
                    let destination = socket2::SockAddr::from(destination);
                    // SAFETY: A native IP address fits the initialized, aligned storage in this request. The box cannot move until retirement.
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            destination.as_ptr().cast::<u8>(),
                            address.cast(),
                            destination.len() as usize,
                        );
                    }
                    header.msg_name = address.cast();
                    header.msg_namelen = destination.len();
                }
                opcode::SendMsg::new(descriptor, self.header.get())
                    .flags(
                        (libc::MSG_NOSIGNAL
                            | if flags.dont_route {
                                libc::MSG_DONTROUTE
                            } else {
                                0
                            }) as _,
                    )
                    .build()
            }
            MessageOperation::Receive(flags) => {
                header.msg_name = address.cast();
                header.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
                opcode::RecvMsg::new(descriptor, self.header.get())
                    .flags(if flags.peek { libc::MSG_PEEK as _ } else { 0 })
                    .build()
            }
        }
        .user_data(self.identifier)
    }

    fn finish(mut self: Box<Self>, result: io::Result<usize>) {
        // Original completion (or an unsubmitted failure) ends all kernel access before these exclusive borrows or owner moves.
        let result = result.map(|bytes| self.received(bytes));
        let reply = self
            .reply
            .take()
            .expect("unfinished request retains its sender");
        let buffers = self
            .buffers
            .take()
            .expect("request returns its buffers once")
            .into_inner();
        drop(self);
        discard(reply.send((result, buffers)));
    }

    fn begin_cancellation(&self) -> bool {
        !self.cancelling.replace(true)
    }
    fn take_reply(&self) -> Option<Box<dyn Send>> {
        self.reply
            .take()
            .map(|reply| Box::new(reply) as Box<dyn Send>)
    }
}

impl<B: IoBufSequence> MessageRequest<B> {
    fn received(&mut self, bytes: usize) -> SocketRecvResult {
        let header = self.header.get_mut();
        let address = if header.msg_namelen == 0 {
            None
        } else {
            // SAFETY: Original completion ends kernel access. Storage was zero-initialized and contains the native address and length. Only IP addresses are decoded.
            let (_, address) = unsafe {
                socket2::SockAddr::try_init(|storage, length| {
                    storage
                        .cast::<libc::sockaddr_storage>()
                        .write(*self.address.get_mut());
                    *length = header.msg_namelen;
                    Ok::<(), io::Error>(())
                })
            }
            .expect("copying completed address metadata is infallible");
            address.as_socket()
        };
        SocketRecvResult {
            bytes,
            address,
            truncated: header.msg_flags & libc::MSG_TRUNC != 0,
            control_truncated: header.msg_flags & libc::MSG_CTRUNC != 0,
        }
    }
}

impl<B: IoBufSequence> Drop for MessageRequest<B> {
    fn drop(&mut self) {
        drop(self.socket.take());
        // Only unpublished requests can retain a reply here. InFlight deliberately leaks requests if original completion cannot be established.
        if let Some(reply) = self.reply.take() {
            discard(
                reply.send((
                    Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "selector closed before submission",
                    )),
                    self.buffers
                        .take()
                        .expect("request returns its buffers once")
                        .into_inner(),
                )),
            );
        }
    }
}

pub(crate) fn validate_lengths(
    count: usize,
    mut lengths: impl Iterator<Item = usize>,
) -> io::Result<()> {
    if count > i32::MAX as usize
        || lengths
            .try_fold(0usize, usize::checked_add)
            .is_none_or(|length| length > i32::MAX as usize)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "message exceeds native length limits",
        ));
    }
    Ok(())
}

fn validate<B: IoBufSequence>(buffers: &B) -> io::Result<()> {
    validate_lengths(
        buffers.buffer_count(),
        (0..buffers.buffer_count()).map(|index| buffers.buffer(index).len()),
    )
}

fn empty_vector(vectors: &mut Vec<libc::iovec>) -> (*mut libc::iovec, usize) {
    // A single empty vector preserves empty datagrams on kernels that reject zero vectors.
    vectors.push(libc::iovec {
        iov_base: std::ptr::NonNull::<u8>::dangling().as_ptr().cast(),
        iov_len: 0,
    });
    (vectors.as_mut_ptr(), 1)
}

fn sending<B: IoBufSequence>(
    buffers: &mut B,
    vectors: &mut Vec<libc::iovec>,
) -> (*mut libc::iovec, usize) {
    if buffers.buffer_count() == 0 {
        return empty_vector(vectors);
    }
    if let Some(slices) = buffers.io_slices() {
        // IoSlice has the native iovec layout. Inline arrays are stable because their sequence lives in the immobile request, while IoBuffers can reuse its existing allocation.
        return (slices.as_ptr().cast_mut().cast(), slices.len());
    }
    vectors.extend((0..buffers.buffer_count()).map(|index| {
        let bytes = buffers.buffer(index);
        libc::iovec {
            iov_base: bytes.as_ptr().cast_mut().cast(),
            iov_len: bytes.len(),
        }
    }));
    (vectors.as_mut_ptr(), vectors.len())
}

fn receiving<B: IoBufSequenceMut>(
    buffers: &mut B,
    vectors: &mut Vec<libc::iovec>,
) -> (*mut libc::iovec, usize) {
    let count = buffers.buffer_count();
    if count == 0 {
        return empty_vector(vectors);
    }
    if let Some(slices) = buffers.io_slices_mut() {
        return (slices.as_ptr().cast_mut().cast(), slices.len());
    }
    for index in 0..count {
        let bytes = buffers.buffer_mut(index);
        vectors.push(libc::iovec {
            iov_base: bytes.as_mut_ptr().cast(),
            iov_len: bytes.len(),
        });
    }
    (vectors.as_mut_ptr(), vectors.len())
}

impl IoUringSelector {
    async fn message_operation<B: IoBufSequence>(
        &self,
        controller: Arc<Controller>,
        socket: &impl SocketSource,
        buffers: B,
        operation: MessageOperation,
        prepare: Prepare<B>,
        cancellation: &Cancellation,
    ) -> MessageResult<B> {
        if let Err(error) = self.check_open() {
            return (Err(error), buffers);
        }
        let permit = match controller.capacity.acquire(cancellation).await {
            Ok(permit) => permit,
            Err(error) => return (Err(error), buffers),
        };
        let identifier = match controller.next_identifier() {
            Ok(identifier) => identifier,
            Err(error) => return (Err(error), buffers),
        };
        let (reply, result) = oneshot::channel();
        let request = Box::new(MessageRequest {
            identifier,
            _permit: permit,
            socket: Some(retain_socket(socket)),
            buffers: Some(UnsafeCell::new(buffers)),
            vectors: Vec::new(),
            // SAFETY: Zero is a valid initial representation for native message and address storage. entry() fills pointers before publication.
            header: UnsafeCell::new(unsafe { std::mem::zeroed() }),
            // SAFETY: sockaddr_storage is an integer-only C structure.
            address: UnsafeCell::new(unsafe { std::mem::zeroed() }),
            operation,
            prepare,
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        });
        Self::submit(controller, request, result, cancellation).await
    }
}

impl IoUringSelector {
    pub(crate) async fn socket_sendmsg<B: IoBufSequence>(
        &self,
        socket: &impl SocketSource,
        message: SocketMessage<B>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, SocketMessage<B>) {
        if let Err(error) = cancellation
            .check()
            .map_err(OperationError::from)
            .and_then(|()| validate(&message.buffers).map_err(Into::into))
        {
            return (Err(error), message);
        }
        let controller = self.route();
        let address = message.address;
        let (result, buffers) = retry_operation(
            self,
            &controller,
            socket,
            message.buffers,
            |buffers| {
                self.message_operation(
                    Arc::clone(&controller),
                    socket,
                    buffers,
                    MessageOperation::Send(flags, address),
                    sending,
                    cancellation,
                )
            },
            Interest::Writable,
            cancellation,
        )
        .await;
        (
            operation_result(result, cancellation).map(|result| result.bytes),
            SocketMessage { buffers, address },
        )
    }
}

impl IoUringSelector {
    pub(crate) async fn socket_recvmsg<B: IoBufSequenceMut>(
        &self,
        socket: &impl SocketSource,
        buffers: B,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        if let Err(error) = cancellation
            .check()
            .map_err(OperationError::from)
            .and_then(|()| validate(&buffers).map_err(Into::into))
        {
            return (Err(error), buffers);
        }
        let controller = self.route();
        let (result, buffers) = retry_operation(
            self,
            &controller,
            socket,
            buffers,
            |buffers| {
                self.message_operation(
                    Arc::clone(&controller),
                    socket,
                    buffers,
                    MessageOperation::Receive(flags),
                    receiving,
                    cancellation,
                )
            },
            Interest::Readable,
            cancellation,
        )
        .await;
        (operation_result(result, cancellation), buffers)
    }
}

#[cfg(test)]
mod tests;
