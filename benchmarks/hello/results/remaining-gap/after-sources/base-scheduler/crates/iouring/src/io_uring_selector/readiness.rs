// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::socket::retain_socket;
use super::*;
use crate::Interest;

// Transient retry waits retain the owned input and socket through retirement. The kernel accesses only the socket; no borrowed payload or I/O-vector pointer enters this request.
struct ReadinessRequest<Input: Send + 'static> {
    identifier: u64,
    _permit: Permit,
    socket: Option<RegisteredSocket>,
    interest: Interest,
    input: Option<Input>,
    reply: Cell<Option<oneshot::Sender<RawBufferResult<Input>>>>,
    cancelling: Cell<bool>,
}

impl<Input: Send + 'static> PendingRequest for ReadinessRequest<Input> {
    fn identifier(&self) -> u64 {
        self.identifier
    }

    fn entry(&mut self) -> squeue::Entry {
        let socket = self.socket.as_ref().expect("request retains its socket");
        let events = match self.interest {
            Interest::Readable => libc::POLLIN,
            Interest::Writable => libc::POLLOUT,
        };
        opcode::PollAdd::new(types::Fd(socket.get_ref().as_raw_fd()), events as u32)
            .build()
            .user_data(self.identifier)
    }

    fn entry_may_wake(&self) -> bool {
        // Only fixed native socket metadata is accessed; no buffer callbacks.
        false
    }

    fn finish(mut self: Box<Self>, result: io::Result<usize>) {
        let reply = self
            .reply
            .take()
            .expect("unfinished request retains its sender");
        let input = self.input.take().unwrap();
        // Release socket and capacity before returning the retained owner, whose destructor may publish lane retirement.
        drop(self);
        discard(reply.send((result, input)));
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

impl<Input: Send + 'static> Drop for ReadinessRequest<Input> {
    fn drop(&mut self) {
        drop(self.socket.take());
        // An unpublished request may be discarded during shutdown. Unknown in-flight requests follow the driver's existing retention policy.
        if let Some(reply) = self.reply.take() {
            discard(reply.send((
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "selector closed before readiness submission",
                )),
                self.input.take().unwrap(),
            )));
        }
    }
}

impl IoUringSelector {
    #[cfg(test)]
    pub(super) async fn wait_ready(
        &self,
        controller: Arc<Controller>,
        socket: &impl SocketSource,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.wait_ready_owned(controller, socket, (), interest, cancellation)
            .await
            .0
    }

    pub(super) async fn wait_ready_owned<Input: Send + 'static>(
        &self,
        controller: Arc<Controller>,
        socket: &impl SocketSource,
        input: Input,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> (OperationResult<()>, Input) {
        if let Err(error) = cancellation
            .check()
            .map_err(OperationError::from)
            .and_then(|()| self.check_open().map_err(Into::into))
        {
            return (Err(error), input);
        }
        let permit = match controller.capacity.acquire(cancellation).await {
            Ok(permit) => permit,
            Err(error) => return (operation_result(Err(error), cancellation), input),
        };
        let identifier = match controller.next_identifier() {
            Ok(identifier) => identifier,
            Err(error) => return (Err(error.into()), input),
        };
        let (reply, result) = oneshot::channel();
        let request = ReadinessRequest {
            identifier,
            _permit: permit,
            socket: Some(retain_socket(socket)),
            interest,
            input: Some(input),
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        };
        let (result, input) =
            Self::submit(controller, Box::new(request), result, cancellation).await;
        (operation_result(result, cancellation).map(drop), input)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod rejection_tests;
