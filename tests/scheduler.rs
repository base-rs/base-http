// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A borrowed bound reader exercises bodies against the released scheduler API.
//! The caller owns reader retirement and exclusive file closure.

use base_http::body::{BodyFrame, ReadableBody};
use base_scheduler::{
    Cancellation, FileClose, FileOpen, FileOptions, FileReader, FileReading, OperationError,
};
use base_scheduler_direct::{DirectOptions, DirectScheduler};
use futures_lite::future::block_on;
use std::io;

struct FileBody<'a, R> {
    reader: &'a mut R,
    finished: bool,
    reading: bool,
}

impl<R: FileReader> ReadableBody for FileBody<'_, R> {
    type Data = Vec<u8>;
    type Error = OperationError;

    fn is_empty(&self) -> bool {
        self.finished
    }

    fn close(&mut self, _error: Option<Self::Error>) {
        self.finished = true;
    }
    async fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<BodyFrame<Self::Data>, Self::Error> {
        cancellation.check()?;
        if self.reading {
            return Err(io::Error::other("previous body read was abandoned").into());
        }
        if self.finished {
            return Ok(BodyFrame::End);
        }
        self.reading = true;
        let (result, mut buffer) = self.reader.read(vec![0; 3], cancellation).await;
        self.reading = false;
        let count = result?;
        if count == 0 {
            self.finished = true;
            Ok(BodyFrame::End)
        } else {
            buffer.truncate(count);
            Ok(BodyFrame::Data {
                data: buffer,
                end: false,
            })
        }
    }
}

async fn next_chunk<B: ReadableBody>(
    body: &mut B,
    cancellation: &Cancellation,
) -> Result<Option<B::Data>, B::Error> {
    Ok(match body.read(cancellation).await? {
        base_http::body::BodyFrame::Data { data, .. } => Some(data),
        base_http::body::BodyFrame::End => None,
        base_http::body::BodyFrame::Headers { .. } => {
            panic!("unexpected fields in a data-only fixture")
        }
    })
}

#[test]
fn file_body_uses_a_bound_reader_with_separate_async_cleanup() {
    fn require_send<T: Send>(future: T) -> T {
        future
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("body");
    std::fs::write(&path, b"abcdefgh").unwrap();
    let scheduler = DirectScheduler::new(DirectOptions {
        threads: 1,
        capacity: 2,
    })
    .unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(async {
            let never = Cancellation::never();
            let file = scheduler
                .file_open(&path, FileOptions::READ, &never)
                .await
                .unwrap();
            let mut reader = scheduler.file_reader(&file).unwrap();
            {
                let mut body = FileBody {
                    reader: &mut reader,
                    finished: false,
                    reading: false,
                };
                drop(next_chunk(&mut body, &never));
                assert_eq!(
                    require_send(next_chunk(&mut body, &never)).await.unwrap(),
                    Some(b"abc".to_vec())
                );
                let cancelled = Cancellation::new();
                cancelled.cancel();
                assert!(matches!(
                    next_chunk(&mut body, &cancelled).await,
                    Err(OperationError::Cancelled(_))
                ));
                assert_eq!(
                    next_chunk(&mut body, &never).await.unwrap(),
                    Some(b"def".to_vec())
                );
                assert_eq!(
                    next_chunk(&mut body, &never).await.unwrap(),
                    Some(b"gh".to_vec())
                );
                assert_eq!(next_chunk(&mut body, &never).await.unwrap(), None);
                assert!(body.is_empty());
                body.close(None);
            }
            reader.close().await.unwrap();
            drop(reader);
            scheduler.file_close(file).await.unwrap();
        });
    }));
    block_on(scheduler.shutdown());
    drop(directory);
    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}
