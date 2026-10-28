// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::SocketMessage;
use crate::{DirectOptions, DirectScheduler, DirectSocket};
use base_scheduler::*;
use base_scheduler_conformance::socket;
use futures_channel::oneshot;
use futures_lite::future::block_on;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(20);

fn run(test: impl FnOnce() + Send + 'static) {
    let (done, completion) = mpsc::channel();
    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(test));
        let _ = done.send(result);
    });
    if let Err(payload) = completion
        .recv_timeout(TIMEOUT)
        .expect("socket test deadline exceeded")
    {
        std::panic::resume_unwind(payload);
    }
}

fn scheduler() -> DirectScheduler {
    DirectScheduler::new(DirectOptions {
        threads: 1,
        capacity: 1,
    })
    .unwrap()
}

fn run_stream(
    domain: SocketDomain,
    test: impl AsyncFnOnce(&DirectScheduler, &DirectSocket, &DirectSocket) + Send + 'static,
) {
    run(move || {
        let scheduler = scheduler();
        let (client, server) = block_on(socket::stream_pair(&scheduler, domain));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            block_on(test(&scheduler, &client, &server))
        }));
        block_on(async {
            if result.is_ok() {
                scheduler.socket_close(client).await.unwrap();
                scheduler.socket_close(server).await.unwrap();
            }
            scheduler.shutdown().await;
        });
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}

fn run_datagram(
    domain: SocketDomain,
    test: impl AsyncFnOnce(&DirectScheduler, &DirectSocket, &DirectSocket) + Send + 'static,
) {
    run(move || {
        let scheduler = scheduler();
        let never = Cancellation::never();
        let local: SocketAddress = match domain {
            SocketDomain::Ipv4 => "127.0.0.1:0",
            SocketDomain::Ipv6 => "[::1]:0",
        }
        .parse()
        .unwrap();
        let (sender, receiver) = block_on(async {
            let sender = scheduler
                .socket_open(
                    domain,
                    SocketType::Datagram,
                    Some(SocketProtocol::Udp),
                    &never,
                )
                .await
                .unwrap();
            let receiver = scheduler
                .socket_open(
                    domain,
                    SocketType::Datagram,
                    Some(SocketProtocol::Udp),
                    &never,
                )
                .await
                .unwrap();
            scheduler.socket_bind(&sender, local, &never).await.unwrap();
            scheduler
                .socket_bind(&receiver, local, &never)
                .await
                .unwrap();
            (sender, receiver)
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            block_on(test(&scheduler, &sender, &receiver))
        }));
        block_on(async {
            if result.is_ok() {
                scheduler.socket_close_submit(sender).await.unwrap();
                scheduler.socket_close(receiver).await.unwrap();
            }
            scheduler.shutdown().await;
        });
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}

mod ipv4 {
    use super::*;
    fn run_stream_case(
        test: impl AsyncFnOnce(&DirectScheduler, &DirectSocket, &DirectSocket) + Send + 'static,
    ) {
        run_stream(SocketDomain::Ipv4, test);
    }
    fn run_datagram_case(
        test: impl AsyncFnOnce(&DirectScheduler, &DirectSocket, &DirectSocket) + Send + 'static,
    ) {
        run_datagram(SocketDomain::Ipv4, test);
    }
    #[test]
    fn pooled_stream_receiver() {
        run_stream_case(async |scheduler, sender, socket| {
            base_scheduler_conformance::socket_receiving::demand_driven(scheduler, sender, socket)
                .await;
            base_scheduler_conformance::socket_receiving::stream(scheduler, sender, socket).await;
        });
    }
    #[test]
    fn pooled_datagram_receiver() {
        run_datagram_case(async |scheduler, sender, socket| {
            base_scheduler_conformance::socket_poll::datagram_first_poll(scheduler, sender, socket)
                .await;
            base_scheduler_conformance::socket_receiving::datagram(scheduler, sender, socket).await;
        });
    }
    mod borrowed_scalar {
        use super::*;
        base_scheduler_conformance::socket_poll_tests!(run: run_stream_case);
    }
    mod borrowed_messages {
        use super::*;
        base_scheduler_conformance::socket_message_poll_tests!(run: run_stream_case);
    }
    mod borrowed_datagrams {
        use super::*;
        base_scheduler_conformance::socket_datagram_poll_tests!(run: run_datagram_case);
    }
    mod packet_handles {
        use super::*;
        base_scheduler_conformance::socket_packet_io_tests!(run: run_datagram_case);
    }
    mod scalar {
        use super::*;
        base_scheduler_conformance::socket_tests!(run: run_stream_case);
    }
    mod messages {
        use super::*;
        base_scheduler_conformance::socket_message_tests!(run: run_stream_case);
    }
    mod datagrams {
        use super::*;
        base_scheduler_conformance::socket_datagram_tests!(run: run_datagram_case);
    }
}

mod ipv6 {
    use super::*;
    fn run_stream_case(
        test: impl AsyncFnOnce(&DirectScheduler, &DirectSocket, &DirectSocket) + Send + 'static,
    ) {
        run_stream(SocketDomain::Ipv6, test);
    }
    fn run_datagram_case(
        test: impl AsyncFnOnce(&DirectScheduler, &DirectSocket, &DirectSocket) + Send + 'static,
    ) {
        run_datagram(SocketDomain::Ipv6, test);
    }
    #[test]
    fn pooled_stream_receiver() {
        run_stream_case(async |scheduler, sender, socket| {
            base_scheduler_conformance::socket_receiving::demand_driven(scheduler, sender, socket)
                .await;
            base_scheduler_conformance::socket_receiving::stream(scheduler, sender, socket).await;
        });
    }
    #[test]
    fn pooled_datagram_receiver() {
        run_datagram_case(async |scheduler, sender, socket| {
            base_scheduler_conformance::socket_poll::datagram_first_poll(scheduler, sender, socket)
                .await;
            base_scheduler_conformance::socket_receiving::datagram(scheduler, sender, socket).await;
        });
    }
    mod borrowed_scalar {
        use super::*;
        base_scheduler_conformance::socket_poll_tests!(run: run_stream_case);
    }
    mod borrowed_messages {
        use super::*;
        base_scheduler_conformance::socket_message_poll_tests!(run: run_stream_case);
    }
    mod borrowed_datagrams {
        use super::*;
        base_scheduler_conformance::socket_datagram_poll_tests!(run: run_datagram_case);
    }
    mod packet_handles {
        use super::*;
        base_scheduler_conformance::socket_packet_io_tests!(run: run_datagram_case);
    }
    mod scalar {
        use super::*;
        base_scheduler_conformance::socket_tests!(run: run_stream_case);
    }
    mod messages {
        use super::*;
        base_scheduler_conformance::socket_message_tests!(run: run_stream_case);
    }
    mod datagrams {
        use super::*;
        base_scheduler_conformance::socket_datagram_tests!(run: run_datagram_case);
    }
}

#[test]
fn sockets_move_between_threads_and_compatible_scheduler_instances() {
    run(|| {
        block_on(async {
            let first = scheduler();
            let second = scheduler();
            let (client, server) = socket::stream_pair(&first, SocketDomain::Ipv4).await;
            std::thread::scope(|scope| {
                let reader = scope.spawn(|| {
                    block_on(async {
                        let mut received = Vec::new();
                        while received.len() < 3 {
                            let (result, buffer) = second
                                .socket_read(
                                    &server,
                                    vec![0; 3 - received.len()],
                                    &Cancellation::never(),
                                )
                                .await;
                            let count = result.unwrap();
                            assert!(count > 0);
                            received.extend_from_slice(&buffer[..count]);
                        }
                        received
                    })
                });
                let mut sent = 0;
                let mut buffer = b"abc".to_vec();
                while sent < 3 {
                    let (result, returned) = block_on(first.socket_write(
                        &client,
                        buffer.slice(sent..),
                        &Cancellation::never(),
                    ));
                    let count = result.unwrap();
                    assert!(count > 0);
                    sent += count;
                    buffer = returned.into_inner();
                }
                assert_eq!(reader.join().unwrap(), b"abc");
            });
            second.socket_close(client).await.unwrap();
            first.socket_close(server).await.unwrap();
            first.shutdown().await;
            second.shutdown().await;
        })
    });
}

struct Gate {
    started: Mutex<Option<oneshot::Sender<()>>>,
    release: mpsc::Receiver<()>,
    dropped: Arc<AtomicBool>,
    retired: Option<oneshot::Sender<()>>,
}
impl Gate {
    fn wait(&self) {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
            self.release.recv_timeout(TIMEOUT).unwrap();
        }
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
        let _ = self.retired.take().unwrap().send(());
    }
}
struct Buffer {
    bytes: Vec<u8>,
    gate: Gate,
}
impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for Buffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.gate.wait();
        &mut self.bytes
    }
}
// SAFETY: The owner retains exclusive stable initialized heap storage. The gate only delays access, without changing its address or range.
unsafe impl IoBuf for Buffer {}
unsafe impl IoBufMut for Buffer {}

struct Sequence {
    buffers: IoBuffersMut<[Vec<u8>; 2]>,
    gate: Gate,
}
// SAFETY: The wrapper retains the prepared owner and forwards its stable initialized ranges and vectors unchanged. The gate only delays synchronous access.
unsafe impl IoBufSequence for Sequence {
    fn buffer_count(&self) -> usize {
        self.buffers.buffer_count()
    }
    fn buffer(&self, index: usize) -> &[u8] {
        self.buffers.buffer(index)
    }
    fn io_slices(&self) -> Option<&[std::io::IoSlice<'_>]> {
        self.gate.wait();
        self.buffers.io_slices()
    }
}
// SAFETY: Exclusive access to the wrapper forwards exclusive access to its prepared mutable owner without overlapping ranges.
unsafe impl IoBufSequenceMut for Sequence {
    fn buffer_mut(&mut self, index: usize) -> &mut [u8] {
        self.buffers.buffer_mut(index)
    }
    fn io_slices_mut(&mut self) -> Option<&[std::io::IoSliceMut<'_>]> {
        self.gate.wait();
        self.buffers.io_slices_mut()
    }
}

enum Input {
    Read(Buffer),
    Recv(Sequence),
    Send(Sequence),
}
impl Input {
    fn pointers(&self) -> Vec<usize> {
        match self {
            Self::Read(buffer) => vec![buffer.bytes.as_ptr() as usize],
            Self::Recv(sequence) | Self::Send(sequence) => (0..sequence.buffer_count())
                .map(|i| sequence.buffer(i).as_ptr() as usize)
                .collect(),
        }
    }
}
struct Guard {
    start: Option<oneshot::Receiver<()>>,
    release: Option<mpsc::Sender<()>>,
    retired: Option<oneshot::Receiver<()>>,
}
impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}
impl base_scheduler_conformance::SocketIoGuard for Guard {
    fn release(&mut self) {
        let _ = self.release.take().unwrap().send(());
    }
    async fn retired(&mut self) {
        self.retired.take().unwrap().await.unwrap();
    }
}
struct Fixture<'a> {
    scheduler: &'a DirectScheduler,
    socket: &'a DirectSocket,
    input: Mutex<Option<Input>>,
    guard: Mutex<Option<Guard>>,
    dropped: Arc<AtomicBool>,
    pointers: Vec<usize>,
}
impl<'a> Fixture<'a> {
    fn new(scheduler: &'a DirectScheduler, socket: &'a DirectSocket, mode: usize) -> Self {
        let (started, start) = oneshot::channel();
        let (release, released) = mpsc::channel();
        let (retired, retirement) = oneshot::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let gate = Gate {
            started: Mutex::new(Some(started)),
            release: released,
            dropped: Arc::clone(&dropped),
            retired: Some(retired),
        };
        let input = match mode {
            0 => Input::Read(Buffer {
                bytes: vec![0; 1],
                gate,
            }),
            1 => Input::Recv(Sequence {
                buffers: IoBuffersMut::new([vec![0; 1], vec![0; 1]]),
                gate,
            }),
            _ => Input::Send(Sequence {
                buffers: IoBuffersMut::new([vec![3], vec![4]]),
                gate,
            }),
        };
        let pointers = input.pointers();
        Self {
            scheduler,
            socket,
            input: Mutex::new(Some(input)),
            guard: Mutex::new(Some(Guard {
                start: Some(start),
                release: Some(release),
                retired: Some(retirement),
            })),
            dropped,
            pointers,
        }
    }
}
impl base_scheduler_conformance::SocketIoFixture for Fixture<'_> {
    type Input = Input;
    type Output = usize;
    type Guard = Guard;
    fn input(&self) -> Input {
        self.input.lock().unwrap().take().unwrap()
    }
    fn assert_retained(&self) {
        assert!(!self.dropped.load(Ordering::SeqCst));
    }
    fn assert_released(&self) {
        assert!(self.dropped.load(Ordering::SeqCst));
    }
    fn assert_completed(&self, bytes: &usize, input: &Input) {
        assert!(*bytes > 0);
        assert_eq!(input.pointers(), self.pointers);
    }
    async fn operation(
        &self,
        input: Input,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, Input) {
        // Require more than the initial poll to submit, exercising lifecycle assertions under delayed admission.
        futures_lite::future::yield_now().await;
        match input {
            Input::Read(buffer) => {
                let (result, buffer) = self
                    .scheduler
                    .socket_read(self.socket, buffer, cancellation)
                    .await;
                (result, Input::Read(buffer))
            }
            Input::Recv(sequence) => {
                let (result, sequence) = self
                    .scheduler
                    .socket_recvmsg(
                        self.socket,
                        sequence,
                        SocketRecvFlags::default(),
                        cancellation,
                    )
                    .await;
                (result.map(|result| result.bytes), Input::Recv(sequence))
            }
            Input::Send(sequence) => {
                let (result, message) = self
                    .scheduler
                    .socket_sendmsg(
                        self.socket,
                        SocketMessage {
                            buffers: sequence,
                            address: None,
                        },
                        SocketSendFlags::default(),
                        cancellation,
                    )
                    .await;
                (result, Input::Send(message.buffers))
            }
        }
    }
    async fn start(&self) -> Guard {
        let mut guard = self.guard.lock().unwrap().take().unwrap();
        guard.start.take().unwrap().await.unwrap();
        guard
    }
}
fn run_io(mode: usize, test: impl AsyncFnOnce(&Fixture<'_>) + Send + 'static) {
    run(move || {
        let scheduler = scheduler();
        let (client, server) = block_on(socket::stream_pair(&scheduler, SocketDomain::Ipv4));
        if mode != 2 {
            let (result, _) =
                block_on(scheduler.socket_write(&client, vec![1, 2], &Cancellation::never()));
            assert!(result.unwrap() > 0);
            // Sending completes when the local kernel accepts bytes. Wait until they reach the peer before gating its first read attempt.
            let (result, _) = block_on(scheduler.socket_recvmsg(
                &server,
                [vec![0; 2]],
                SocketRecvFlags { peek: true },
                &Cancellation::never(),
            ));
            assert!(result.unwrap().bytes > 0);
        }
        let fixture = Fixture::new(&scheduler, &server, mode);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&fixture))));
        drop(fixture);
        block_on(async {
            if result.is_ok() {
                scheduler.socket_close(client).await.unwrap();
                scheduler.socket_close(server).await.unwrap();
            }
            scheduler.shutdown().await;
        });
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}

mod scalar_read_lifecycle {
    use super::*;
    fn run_case(test: impl AsyncFnOnce(&Fixture<'_>) + Send + 'static) {
        run_io(0, test);
    }
    base_scheduler_conformance::socket_io_tests!(run: run_case);
}
mod prepared_receive_lifecycle {
    use super::*;
    fn run_case(test: impl AsyncFnOnce(&Fixture<'_>) + Send + 'static) {
        run_io(1, test);
    }
    base_scheduler_conformance::socket_io_tests!(run: run_case);
}
mod prepared_send_lifecycle {
    use super::*;
    fn run_case(test: impl AsyncFnOnce(&Fixture<'_>) + Send + 'static) {
        run_io(2, test);
    }
    base_scheduler_conformance::socket_io_tests!(run: run_case);
}

#[test]
fn failed_writes_return_the_original_buffer() {
    run(|| {
        block_on(async {
            let scheduler = scheduler();
            let socket = scheduler
                .socket_open(
                    SocketDomain::Ipv4,
                    SocketType::Stream,
                    None,
                    &Cancellation::never(),
                )
                .await
                .unwrap();
            socket::write_error(&scheduler, &socket).await;
            scheduler.socket_close(socket).await.unwrap();
            scheduler.shutdown().await;
        })
    });
}

// Existing callers can keep the aggregate helpers while migrating to generated suites.
#[test]
fn aggregate_socket_helpers_remain_compatible() {
    run_stream(SocketDomain::Ipv4, async |scheduler, sender, receiver| {
        socket::stream(scheduler, sender, receiver).await;
    });
    run_stream(SocketDomain::Ipv4, async |scheduler, sender, receiver| {
        base_scheduler_conformance::socket_messages::stream(scheduler, sender, receiver).await;
    });
    run_datagram(SocketDomain::Ipv4, async |scheduler, sender, receiver| {
        base_scheduler_conformance::socket_messages::datagrams(scheduler, sender, receiver).await;
    });
}

#[test]
fn directional_socket_handles_reuse_caller_owned_storage() {
    run_stream(SocketDomain::Ipv4, async |scheduler, sender, receiver| {
        base_scheduler_conformance::socket_resources::stream(scheduler, sender, receiver).await;
    });
}
