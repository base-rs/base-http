// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#[path = "support/socket_model.rs"]
mod socket_model;
use base_scheduler::*;
use base_scheduler_conformance::{blocking, file};
use base_scheduler_conformance::{socket, socket_messages};
use futures_lite::future::block_on;
use socket_model::{SocketModel, socket_factories};
use std::{
    future::{Ready, ready},
    panic::{AssertUnwindSafe, catch_unwind},
};

// Intentionally broken implementations establish that the reusable suite rejects violations.
struct IncorrectFile;
impl FileOperation for IncorrectFile {
    type File = ();
}
impl FileReader for IncorrectFile {
    async fn read<B: IoBufMut>(&mut self, buffer: B, _: &Cancellation) -> BufferResult<B> {
        (Ok(0), buffer)
    }
    async fn read_at<B: IoBufMut>(
        &mut self,
        buffer: B,
        _: u64,
        _: &Cancellation,
    ) -> BufferResult<B> {
        (Ok(0), buffer)
    }
    async fn close(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl FileReading for IncorrectFile {
    type Reader<'a> = Self;
    fn file_reader(&self, _: &()) -> std::io::Result<Self> {
        Ok(Self)
    }
}
impl FileWriter for IncorrectFile {
    async fn write<B: IoBuf>(&mut self, buffer: B, _: &Cancellation) -> BufferResult<B> {
        (Ok(buffer.as_ref().len()), buffer)
    }
    async fn write_at<B: IoBuf>(&mut self, buffer: B, _: u64, _: &Cancellation) -> BufferResult<B> {
        (Ok(buffer.as_ref().len()), buffer)
    }
    async fn close(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl FileWriting for IncorrectFile {
    type Writer<'a> = Self;
    fn file_writer(&self, _: &()) -> std::io::Result<Self> {
        Ok(Self)
    }
}
#[test]
#[should_panic(expected = "expected cooperative cancellation")]
fn rejects_a_read_that_ignores_cancellation() {
    block_on(file::read(&IncorrectFile, &(), b"data"));
}
#[test]
#[should_panic]
fn rejects_a_write_that_loses_an_expected_error() {
    block_on(file::write_error(&IncorrectFile, &()));
}

#[derive(Clone, Copy)]
enum Fault {
    Eager,
    IgnoreCancellation,
    LosePanic,
}
struct IncorrectBlocking(Fault);
impl Blocking for IncorrectBlocking {
    type Handle<T>
        = Ready<Result<T, TaskError>>
    where
        T: Send + 'static;
    fn spawn_blocking<F, T>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> impl std::future::Future<Output = Result<Self::Handle<T>, BlockingError>> + Send
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let mode = self.0;
        let mut operation = Some(operation);
        let eager = if matches!(mode, Fault::Eager) {
            Some(
                catch_unwind(AssertUnwindSafe(operation.take().unwrap()))
                    .map_err(TaskError::Panicked),
            )
        } else {
            None
        };
        async move {
            if let Some(result) = eager {
                return Ok(ready(result));
            }
            if cancellation.is_cancelled() && !matches!(mode, Fault::IgnoreCancellation) {
                return Err(BlockingError::Cancelled);
            }
            let result =
                catch_unwind(AssertUnwindSafe(operation.take().unwrap())).map_err(|payload| {
                    if matches!(mode, Fault::LosePanic) {
                        TaskError::Cancelled
                    } else {
                        TaskError::Panicked(payload)
                    }
                });
            Ok(ready(result))
        }
    }
}

#[test]
fn rejects_eager_submission_ignored_cancellation_and_lost_panics() {
    for fault in [Fault::Eager, Fault::IgnoreCancellation, Fault::LosePanic] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| block_on(blocking::lifecycle(
                &IncorrectBlocking(fault)
            ))))
            .is_err()
        );
    }
}

struct IncorrectSocket;
impl SocketModel for IncorrectSocket {
    fn recv(&self, _: &(), _: &mut [u8], _: &Cancellation) -> OperationResult<usize> {
        Ok(0)
    }
    fn send(&self, _: &(), _: &[u8], _: &Cancellation) -> OperationResult<usize> {
        Ok(0)
    }
}
socket_factories!(IncorrectSocket);
impl SocketOperation for IncorrectSocket {
    type Socket = ();
}

impl SocketShutdown for IncorrectSocket {
    async fn socket_shutdown(&self, _: &(), _: std::net::Shutdown) -> std::io::Result<()> {
        Ok(())
    }
    async fn socket_shutdown_submit(&self, _: &(), _: std::net::Shutdown) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn socket_conformance_rejects_ignored_cancellation_and_lost_os_errors() {
    assert!(catch_unwind(|| block_on(socket::stream(&IncorrectSocket, &(), &()))).is_err());
    assert!(catch_unwind(|| block_on(socket::write_error(&IncorrectSocket, &()))).is_err());
    assert!(
        catch_unwind(|| block_on(socket_messages::stream(&IncorrectSocket, &(), &()))).is_err()
    );
}

// Script a supported short-send outcome independently of the host TCP buffering policy.
struct ShortSend(usize);
impl SocketOperation for ShortSend {
    type Socket = ();
}
impl SocketModel for ShortSend {
    fn recv(&self, _: &(), _: &mut [u8], _: &Cancellation) -> OperationResult<usize> {
        Ok(0)
    }
    fn send(&self, _: &(), _: &[u8], _: &Cancellation) -> OperationResult<usize> {
        Ok(self.0)
    }
}
socket_factories!(ShortSend);

#[test]
fn short_send_assertions_cover_advancement_and_reject_zero_and_full_counts() {
    block_on(socket_messages::partial_send(
        &ShortSend(3),
        &(),
        [vec![1; 2], vec![2; 3]],
    ));
    for count in [0, 5] {
        assert!(
            catch_unwind(|| block_on(socket_messages::partial_send(
                &ShortSend(count),
                &(),
                [vec![1; 2], vec![2; 3]]
            )))
            .is_err()
        );
    }
}

// Report cancellation correctly while deliberately performing the forbidden side effect. Each assertion must reject the violation without depending on another case's socket state.
struct SideEffectSocket {
    bytes: std::sync::Mutex<std::collections::VecDeque<u8>>,
    cancelled_write: bool,
    cancelled_read: bool,
}
impl SideEffectSocket {
    fn send(&self, bytes: &[u8], cancellation: &Cancellation) -> OperationResult<usize> {
        if !cancellation.is_cancelled() || self.cancelled_write {
            self.bytes.lock().unwrap().extend(bytes);
        }
        cancellation.check()?;
        Ok(bytes.len())
    }
    fn recv(&self, bytes: &mut [u8], cancellation: &Cancellation) -> OperationResult<usize> {
        let mut count = 0;
        if !cancellation.is_cancelled() || self.cancelled_read {
            let mut queued = self.bytes.lock().unwrap();
            for byte in bytes {
                let Some(next) = queued.pop_front() else {
                    break;
                };
                *byte = next;
                count += 1;
            }
        }
        cancellation.check()?;
        Ok(count)
    }
}
impl SocketModel for SideEffectSocket {
    fn recv(
        &self,
        _: &(),
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.recv(buffer, cancellation)
    }
    fn send(&self, _: &(), buffer: &[u8], cancellation: &Cancellation) -> OperationResult<usize> {
        self.send(buffer, cancellation)
    }
}
socket_factories!(SideEffectSocket);
impl SocketOperation for SideEffectSocket {
    type Socket = ();
}

#[test]
fn isolated_pre_cancelled_assertions_reject_hidden_scalar_and_message_side_effects() {
    for (cancelled_write, cancelled_read) in [(true, false), (false, true)] {
        let fixture = SideEffectSocket {
            bytes: Default::default(),
            cancelled_write,
            cancelled_read,
        };
        assert!(catch_unwind(|| block_on(socket::pre_cancelled(&fixture, &(), &()))).is_err());
        let fixture = SideEffectSocket {
            bytes: Default::default(),
            cancelled_write,
            cancelled_read,
        };
        assert!(
            catch_unwind(|| block_on(socket_messages::pre_cancelled(&fixture, &(), &()))).is_err()
        );
    }
}
