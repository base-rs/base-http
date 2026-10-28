// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{
    Cancellation, IoBufSequence, IoBufSequenceMut, IoBuffers, IoBuffersMut, OperationError,
    OperationResult, SocketOperation, SocketRecvFlags, SocketRecvResult, SocketSendFlags,
};
use futures_channel::oneshot;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

struct SocketMessage<Buffers = Vec<Vec<u8>>> {
    buffers: Buffers,
    address: Option<base_scheduler::SocketAddress>,
}

// A deliberately small message backend: one partial transfer, with no I/O-vector allocation.
struct Messages;

impl SocketOperation for Messages {
    type Socket = bool; // Inject an I/O error independently of cancellation.
}

impl Messages {
    async fn socket_sendmsg<Buffers: IoBufSequence>(
        &self,
        fail: &bool,
        message: SocketMessage<Buffers>,
        _: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, SocketMessage<Buffers>) {
        let result = cancellation
            .check()
            .map_err(OperationError::from)
            .and_then(|()| {
                if *fail {
                    Err(io::Error::from(io::ErrorKind::BrokenPipe).into())
                } else {
                    Ok((0..message.buffers.buffer_count())
                        .map(|index| message.buffers.buffer(index).len())
                        .sum::<usize>()
                        .min(3))
                }
            });
        (result, message)
    }
}

impl Messages {
    async fn socket_recvmsg<Buffers: IoBufSequenceMut>(
        &self,
        fail: &bool,
        mut buffers: Buffers,
        _: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, Buffers) {
        let result = cancellation
            .check()
            .map_err(OperationError::from)
            .and_then(|()| {
                if *fail {
                    return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
                }
                let mut bytes = 0;
                for index in 0..buffers.buffer_count() {
                    for byte in buffers.buffer_mut(index).iter_mut().take(3 - bytes) {
                        *byte = b'a' + bytes as u8;
                        bytes += 1;
                    }
                }
                Ok(SocketRecvResult {
                    bytes,
                    address: None,
                    truncated: false,
                    control_truncated: false,
                })
            });
        (result, buffers)
    }
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("the immediate message fixture must complete on its first poll"),
    }
}

struct PooledBuffers {
    buffers: [Box<[u8]>; 2],
    returned: Arc<AtomicUsize>,
}

impl Drop for PooledBuffers {
    fn drop(&mut self) {
        self.returned.fetch_add(1, Ordering::SeqCst);
    }
}

// SAFETY: This owner retains the same two boxes in order; their byte allocations remain stable even when the owner moves, and no other owner accesses them.
unsafe impl IoBufSequence for PooledBuffers {
    fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    fn buffer(&self, index: usize) -> &[u8] {
        &self.buffers[index]
    }
}

// SAFETY: Exclusive borrowing exposes those same boxes without aliases.
unsafe impl IoBufSequenceMut for PooledBuffers {
    fn buffer_mut(&mut self, index: usize) -> &mut [u8] {
        &mut self.buffers[index]
    }
}

fn exercise<Buffers: IoBufSequenceMut>(buffers: Buffers) {
    let pointers: Vec<_> = (0..buffers.buffer_count())
        .map(|index| buffers.buffer(index).as_ptr())
        .collect();
    let address = Some("127.0.0.1:1234".parse().unwrap());
    let mut buffers = Some(buffers);
    let live = Cancellation::new();
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let allocations = allocation_counter::measure(|| {
        for _ in 0..16 {
            for token in [&live, &cancelled] {
                let message = SocketMessage {
                    buffers: buffers.take().unwrap(),
                    address,
                };
                let (sent, message) = ready(Messages.socket_sendmsg(
                    &false,
                    message,
                    SocketSendFlags::default(),
                    token,
                ));
                assert_eq!(message.address, address);
                let (received, owner) = ready(Messages.socket_recvmsg(
                    &false,
                    message.buffers,
                    SocketRecvFlags::default(),
                    token,
                ));
                if token.is_cancelled() {
                    assert!(matches!(sent, Err(OperationError::Cancelled(_))));
                    assert!(matches!(received, Err(OperationError::Cancelled(_))));
                } else {
                    assert_eq!(sent.unwrap(), 3);
                    assert_eq!(received.unwrap().bytes, 3);
                    assert_eq!(owner.buffer(0), b"ab");
                    assert_eq!(owner.buffer(1), b"c\0");
                }
                for (index, pointer) in pointers.iter().enumerate() {
                    assert_eq!(owner.buffer(index).as_ptr(), *pointer);
                    assert_eq!(owner.buffer(index).len(), 2);
                }
                buffers = Some(owner);
            }
        }
    });
    assert_eq!(allocations.count_total, 0);

    // Errors also recover the same owners, without reconstructing the collection.
    let (result, message) = ready(Messages.socket_sendmsg(
        &true,
        SocketMessage {
            buffers: buffers.take().unwrap(),
            address,
        },
        SocketSendFlags::default(),
        &live,
    ));
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    let (result, owner) =
        ready(Messages.socket_recvmsg(&true, message.buffers, SocketRecvFlags::default(), &live));
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    for (index, pointer) in pointers.iter().enumerate() {
        assert_eq!(owner.buffer(index).as_ptr(), *pointer);
    }
}

#[test]
fn arrays_vectors_boxed_slices_and_pool_owners_compose_without_new_allocations() {
    exercise([vec![0; 2], vec![0; 2]]);
    exercise(vec![vec![0; 2], vec![0; 2]]);
    exercise(vec![vec![0; 2], vec![0; 2]].into_boxed_slice());
    exercise(IoBuffersMut::new([vec![0; 2], vec![0; 2]]));
    let returned = Arc::new(AtomicUsize::new(0));
    exercise(PooledBuffers {
        buffers: [vec![0; 2].into_boxed_slice(), vec![0; 2].into_boxed_slice()],
        returned: Arc::clone(&returned),
    });
    assert_eq!(returned.load(Ordering::SeqCst), 1);
    exercise(IoBuffersMut::new(PooledBuffers {
        buffers: [vec![0; 2].into_boxed_slice(), vec![0; 2].into_boxed_slice()],
        returned: Arc::clone(&returned),
    }));
    assert_eq!(returned.load(Ordering::SeqCst), 2);
}

#[test]
fn empty_arrays_and_default_messages_remain_supported() {
    let message = SocketMessage {
        buffers: [] as [Vec<u8>; 0],
        address: None,
    };
    let (result, message) = ready(Messages.socket_sendmsg(
        &false,
        message,
        SocketSendFlags::default(),
        &Cancellation::never(),
    ));
    assert_eq!(result.unwrap(), 0);
    let (result, buffers) = ready(Messages.socket_recvmsg(
        &false,
        message.buffers,
        SocketRecvFlags::default(),
        &Cancellation::never(),
    ));
    assert_eq!(result.unwrap().bytes, 0);
    assert!(buffers.is_empty());
    let message: SocketMessage = SocketMessage {
        buffers: vec![vec![1]],
        address: None,
    };
    assert_eq!(
        ready(Messages.socket_sendmsg(
            &false,
            message,
            SocketSendFlags::default(),
            &Cancellation::never()
        ))
        .0
        .unwrap(),
        1
    );
}

struct RetainingSender {
    accepted: mpsc::Sender<()>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl SocketOperation for RetainingSender {
    type Socket = ();
}

impl RetainingSender {
    async fn socket_sendmsg<Buffers: IoBufSequence>(
        &self,
        _: &(),
        message: SocketMessage<Buffers>,
        _: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, SocketMessage<Buffers>) {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), message);
        }
        let (completion, result) = oneshot::channel();
        let release = Arc::clone(&self.release);
        let accepted = self.accepted.clone();
        *self.worker.lock().unwrap() = Some(std::thread::spawn(move || {
            accepted.send(()).unwrap();
            release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
            assert_eq!(&*message.buffers.io_slices().unwrap()[0], b"ab");
            let _ = completion.send((Ok(2), message));
        }));
        result.await.unwrap()
    }
}

#[test]
fn abandoning_a_wait_retains_the_entire_collection_until_backend_retirement() {
    let (accepted, admission) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let scheduler = RetainingSender {
        accepted,
        release: Arc::new(Mutex::new(released)),
        worker: Mutex::new(None),
    };
    let returned = Arc::new(AtomicUsize::new(0));
    let message = SocketMessage {
        buffers: IoBuffers::new(PooledBuffers {
            buffers: [Box::from(*b"ab"), Box::from(*b"cd")],
            returned: Arc::clone(&returned),
        }),
        address: None,
    };
    let token = Cancellation::new();
    let mut operation =
        Box::pin(scheduler.socket_sendmsg(&(), message, SocketSendFlags::default(), &token));
    assert!(admission.try_recv().is_err());
    assert!(
        operation
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    admission.recv_timeout(Duration::from_secs(10)).unwrap();
    token.cancel();
    drop(operation);
    assert_eq!(returned.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    scheduler
        .worker
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(returned.load(Ordering::SeqCst), 1);
}
