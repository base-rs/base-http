// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Independent directional resources retain registration privately across repeated operations.
use base_scheduler::*;
use futures_lite::future::{poll_once, zip};

pub async fn stream<S>(scheduler: &S, sender: &S::Socket, receiver: &S::Socket)
where
    S: SocketStreamReading + SocketStreamWriting,
    S::Socket: 'static,
{
    let never = Cancellation::never();
    let mut reader = scheduler.socket_stream_reader(receiver).unwrap();
    let mut writer = scheduler.socket_stream_writer(receiver).unwrap();
    let mut sender_read = scheduler.socket_stream_reader(sender).unwrap();
    let mut sender_write = scheduler.socket_stream_writer(sender).unwrap();
    for byte in *b"ab" {
        let mut data = [0];
        let (read, write) = zip(
            reader.read_into(&mut data, &never),
            sender_write.write(&[byte], &never),
        )
        .await;
        assert_eq!(read.unwrap(), 1);
        assert_eq!(write.unwrap(), 1);
        assert_eq!(data, [byte]);
        let mut returned = [0];
        let (read, write) = zip(
            sender_read.read_into(&mut returned, &never),
            writer.write(&data, &never),
        )
        .await;
        assert_eq!(read.unwrap(), 1);
        assert_eq!(write.unwrap(), 1);
        assert_eq!(returned, [byte]);
    }
    let mut incoming = [0];
    let mut outgoing = [0];
    let (server, client) = zip(
        zip(
            reader.read_into(&mut incoming, &never),
            writer.write(b"s", &never),
        ),
        zip(
            sender_write.write(b"c", &never),
            sender_read.read_into(&mut outgoing, &never),
        ),
    )
    .await;
    assert_eq!(server.0.unwrap(), 1);
    assert_eq!(server.1.unwrap(), 1);
    assert_eq!(client.0.unwrap(), 1);
    assert_eq!(client.1.unwrap(), 1);
    assert_eq!(incoming, *b"c");
    assert_eq!(outgoing, *b"s");
    let cancellation = Cancellation::new();
    let mut data = [0];
    let mut waiting = Box::pin(reader.read_into(&mut data, &cancellation));
    assert!(poll_once(waiting.as_mut()).await.is_none());
    cancellation.cancel();
    cancelled(waiting.await);
    cancelled(reader.read_into(&mut data, &cancellation).await);
    cancelled(writer.write(&data, &cancellation).await);
    let mut pending = Box::pin(reader.read_into(&mut data, &never));
    assert!(poll_once(pending.as_mut()).await.is_none());
    drop(pending);
    let (read, write) = zip(
        reader.read_into(&mut data, &never),
        sender_write.write(b"z", &never),
    )
    .await;
    assert_eq!(read.unwrap(), 1);
    assert_eq!(write.unwrap(), 1);
    assert_eq!(data, *b"z");
    drop(reader.close());
    reader.close().await.unwrap();
    writer.close().await.unwrap();
    sender_read.close().await.unwrap();
    sender_write.close().await.unwrap();
}

pub(crate) fn cancelled<T>(result: OperationResult<T>) {
    assert!(
        matches!(result, Err(OperationError::Cancelled(_))),
        "expected cooperative cancellation"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cancellation_assertion_rejects_progress_and_io_errors() {
        assert!(std::panic::catch_unwind(|| cancelled::<()>(Ok(()))).is_err());
        assert!(
            std::panic::catch_unwind(|| cancelled::<()>(Err(std::io::Error::from(
                std::io::ErrorKind::Interrupted
            )
            .into())))
            .is_err()
        );
    }
}
