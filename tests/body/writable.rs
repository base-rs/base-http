// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::body::{BodyError, BodyFrame, BodyReader, BodyWriter, ReadableBody, WritableBody};
use base_scheduler::Cancellation;
use futures_lite::future::block_on;
use std::{error::Error as _, io};

fn failure(message: &str) -> BodyError {
    BodyError::new(io::Error::other(message.to_owned()))
}

fn source(error: &BodyError) -> String {
    error.source().unwrap().to_string()
}

// Generic producer code receives a `&mut` borrow as its own writer type.
async fn produce<W: WritableBody<Data = Vec<u8>>>(
    mut writer: W,
    cancellation: &Cancellation,
) -> Result<(), W::Error> {
    writer.write(b"one".to_vec(), cancellation).await?;
    writer.flush(cancellation).await?;
    writer
        .write_frame(
            BodyFrame::Data {
                data: b"two".to_vec(),
                end: false,
            },
            cancellation,
        )
        .await?;
    writer.close_write(None);
    Ok(())
}

#[test]
fn borrowed_writers_forward_writes_flushes_and_closure() {
    block_on(async {
        let never = Cancellation::never();
        let (mut body, mut writer) = BodyReader::bounded(16, 4);
        produce(&mut writer, &never).await.unwrap();
        for expected in [&b"one"[..], &b"two"[..]] {
            assert!(
                matches!(body.read(&never).await, Ok(BodyFrame::Data { data, .. }) if data == expected)
            );
        }
        assert!(matches!(body.read(&never).await, Ok(BodyFrame::End)));
        assert!(matches!(
            writer.write(Vec::new(), &never).await,
            Err(BodyError::Closed)
        ));
    });
}

#[test]
fn borrowed_writers_forward_abort_and_failure() {
    fn abort<W: WritableBody>(mut writer: W) {
        writer.abort();
    }
    fn fail<W: WritableBody>(mut writer: W, error: W::Error) {
        writer.close_write(Some(error));
    }
    let never = Cancellation::never();
    let (mut body, mut writer) = BodyReader::<Vec<u8>>::bounded(16, 4);
    block_on(writer.write(vec![1], &never)).unwrap();
    abort(&mut writer);
    assert!(body.is_empty());
    assert!(matches!(
        block_on(body.read(&never)),
        Err(BodyError::Abandoned)
    ));

    let (mut body, mut writer) = BodyReader::<Vec<u8>>::bounded(16, 4);
    fail(&mut writer, failure("producer failed"));
    let error = block_on(body.read(&never)).unwrap_err();
    assert_eq!(source(&error), "producer failed");
}

#[test]
fn inherent_abort_discards_queued_frames_and_reports_abandonment() {
    let never = Cancellation::never();
    let (mut body, mut writer) = BodyReader::<Vec<u8>>::bounded(16, 4);
    block_on(writer.write(vec![1], &never)).unwrap();
    writer.abort();
    assert!(body.is_ready());
    assert!(matches!(
        block_on(body.read(&never)),
        Err(BodyError::Abandoned)
    ));
    assert!(matches!(
        block_on(writer.write(vec![2], &never)),
        Err(BodyError::Abandoned)
    ));
}

#[test]
fn flushing_reports_cancellation_abandonment_and_reader_failure() {
    let never = Cancellation::never();
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let (mut body, mut writer) = BodyReader::<Vec<u8>>::bounded(16, 4);
    block_on(writer.write(vec![1], &never)).unwrap();
    block_on(writer.flush(&never)).unwrap();
    assert!(matches!(
        block_on(writer.flush(&cancelled)),
        Err(BodyError::Cancelled)
    ));
    // Flushing does not consume or close the queue.
    assert!(
        matches!(block_on(body.read(&never)), Ok(BodyFrame::Data { data, end: false }) if data == [1])
    );
    body.close(None);
    assert!(matches!(
        block_on(writer.flush(&never)),
        Err(BodyError::Abandoned)
    ));

    let (mut body, mut writer) = BodyReader::<Vec<u8>>::bounded(16, 4);
    body.close(Some(failure("consumer failed")));
    let error = block_on(writer.flush(&never)).unwrap_err();
    assert_eq!(source(&error), "consumer failed");
}

#[test]
fn reader_failure_is_reported_to_later_writes() {
    let never = Cancellation::never();
    let (mut body, mut writer) = BodyReader::<Vec<u8>>::bounded(16, 4);
    body.close(Some(failure("consumer failed")));
    let error = block_on(writer.write(vec![1], &never)).unwrap_err();
    assert_eq!(source(&error), "consumer failed");
    let error = block_on(body.read(&never)).unwrap_err();
    assert_eq!(source(&error), "consumer failed");
}

#[test]
fn precancelled_writes_leave_the_queue_unchanged() {
    let never = Cancellation::never();
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let (mut body, mut writer) = BodyReader::<Vec<u8>>::bounded(16, 4);
    assert!(matches!(
        block_on(writer.write(vec![1], &cancelled)),
        Err(BodyError::Cancelled)
    ));
    assert!(matches!(
        block_on(writer.write_frame(BodyFrame::End, &cancelled)),
        Err(BodyError::Cancelled)
    ));
    assert!(!body.is_ready());
    writer.close_write(None);
    assert!(matches!(block_on(body.read(&never)), Ok(BodyFrame::End)));
}

#[test]
fn the_first_failure_is_preserved_whichever_side_reports_it() {
    let never = Cancellation::never();
    let (mut body, mut writer) = BodyReader::<Vec<u8>>::bounded(16, 4);
    body.close(Some(failure("consumer failed")));
    writer.close_write(Some(failure("producer failed")));
    let error = block_on(body.read(&never)).unwrap_err();
    assert_eq!(source(&error), "consumer failed");

    let (mut body, mut writer): (BodyReader, BodyWriter) = BodyReader::bounded(16, 4);
    writer.close_write(Some(failure("producer failed")));
    body.close(Some(failure("consumer failed")));
    let error = block_on(body.read(&never)).unwrap_err();
    assert_eq!(source(&error), "producer failed");
}
