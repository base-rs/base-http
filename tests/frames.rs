// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::body::{
    BodyError, BodyFrame, BodyReader, BufferedBody, EmptyBody, ReadableBody, WritableBody,
};
use base_http::{Fields, Method, Request, Response, Status};
use base_scheduler::Cancellation;
use futures_lite::future::{block_on, poll_once};
use std::pin::pin;

// The data owner deliberately implements neither Clone nor Default.
struct Data(Vec<u8>);
impl AsRef<[u8]> for Data {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[test]
fn buffered_frames_report_completion_with_the_final_payload() {
    block_on(async {
        let cancel = Cancellation::never();
        let mut body = BufferedBody::new([b"hello".as_slice(), b"".as_slice()]);
        assert!(matches!(
            body.read(&cancel).await.unwrap(),
            BodyFrame::Data {
                data: b"hello",
                end: false
            }
        ));
        assert!(matches!(
            body.read(&cancel).await.unwrap(),
            BodyFrame::Data {
                data: b"",
                end: true
            }
        ));
        assert!(matches!(body.read(&cancel).await.unwrap(), BodyFrame::End));
        assert!(body.rewind());
        assert!(!body.read(&cancel).await.unwrap().is_end());
        assert!(EmptyBody.read(&cancel).await.unwrap().is_end());
    });
}

#[test]
fn proxying_moves_data_and_fields_without_cloning_their_payloads() {
    block_on(async {
        let cancel = Cancellation::never();
        let (mut input, mut producer) = BodyReader::<Data>::bounded(4096, 2);
        let (mut output, mut writer) = BodyReader::<Data>::bounded(4096, 2);
        let data = Data(vec![42; 128]);
        let data_pointer = data.0.as_ptr();
        let fields = Fields::try_from_pairs([("x-result", vec![b'a'; 512])]).unwrap();
        let field_pointer = fields.pairs()[0].value().as_ptr();
        producer
            .write_frame(BodyFrame::Data { data, end: false }, &cancel)
            .await
            .unwrap();
        producer
            .write_frame(BodyFrame::Headers { fields, end: true }, &cancel)
            .await
            .unwrap();
        drop(producer);

        // Exercise the generic writable trait, rather than relying on an
        // inherent method or boxing the data owner.
        async fn forward<W: WritableBody>(
            writer: &mut W,
            frame: BodyFrame<W::Data>,
            cancel: &Cancellation,
        ) -> Result<(), W::Error> {
            writer.write_frame(frame, cancel).await
        }
        loop {
            let frame = input.read(&cancel).await.unwrap();
            let end = frame.is_end();
            forward(&mut writer, frame, &cancel).await.unwrap();
            if end {
                break;
            }
        }
        drop(writer);
        let BodyFrame::Data { data, end: false } = output.read(&cancel).await.unwrap() else {
            panic!("expected data")
        };
        assert_eq!(data.0.as_ptr(), data_pointer);
        let BodyFrame::Headers { fields, end: true } = output.read(&cancel).await.unwrap() else {
            panic!("expected headers")
        };
        assert_eq!(fields.pairs()[0].value().as_ptr(), field_pointer);
        assert!(matches!(
            output.read(&cancel).await.unwrap(),
            BodyFrame::End
        ));
    });
}

#[test]
fn trailing_fields_can_precede_separately_observed_completion() {
    block_on(async {
        let cancel = Cancellation::never();
        let (mut body, mut writer) = BodyReader::<Data>::bounded(128, 2);
        writer
            .write_frame(
                BodyFrame::Headers {
                    fields: Fields::new(),
                    end: false,
                },
                &cancel,
            )
            .await
            .unwrap();
        assert!(matches!(
            body.read(&cancel).await.unwrap(),
            BodyFrame::Headers { end: false, .. }
        ));
        assert!(!body.is_empty());
        {
            let pending = pin!(body.read(&cancel));
            assert!(poll_once(pending).await.is_none());
        }
        assert!(matches!(
            writer.write(Data(vec![1]), &cancel).await,
            Err(BodyError::InvalidFrame)
        ));
        assert!(matches!(
            writer
                .write_frame(
                    BodyFrame::Headers {
                        fields: Fields::new(),
                        end: true
                    },
                    &cancel
                )
                .await,
            Err(BodyError::InvalidFrame)
        ));
        writer.write_frame(BodyFrame::End, &cancel).await.unwrap();
        assert!(matches!(body.read(&cancel).await.unwrap(), BodyFrame::End));
    });
}

#[test]
fn final_frames_close_output_atomically_and_survive_producer_drop() {
    block_on(async {
        let cancel = Cancellation::never();
        let (mut body, mut writer) = BodyReader::bounded(128, 2);
        writer
            .write_frame(
                BodyFrame::Data {
                    data: vec![1],
                    end: true,
                },
                &cancel,
            )
            .await
            .unwrap();
        assert!(matches!(
            writer.write(vec![2], &cancel).await,
            Err(BodyError::Closed)
        ));
        drop(writer);
        assert!(
            matches!(body.read(&cancel).await.unwrap(), BodyFrame::Data { data, end: true } if data == [1])
        );
        assert!(matches!(body.read(&cancel).await.unwrap(), BodyFrame::End));
        // Completion alone has no requirement to construct a data value.
        let (mut body, mut writer) = BodyReader::<Data>::bounded(1, 1);
        writer.write_frame(BodyFrame::End, &cancel).await.unwrap();
        drop(writer);
        assert!(matches!(body.read(&cancel).await.unwrap(), BodyFrame::End));
    });
}

#[test]
fn cancelled_and_dropped_pending_terminal_writes_do_not_close_output() {
    block_on(async {
        let never = Cancellation::never();
        let (mut body, mut writer) = BodyReader::<Vec<u8>>::bounded(32, 1);
        writer.write(vec![1], &never).await.unwrap();
        let cancel = Cancellation::new();
        {
            let mut pending = pin!(writer.write_frame(
                BodyFrame::Headers {
                    fields: Fields::new(),
                    end: true
                },
                &cancel
            ));
            assert!(poll_once(pending.as_mut()).await.is_none());
            cancel.cancel();
            assert!(matches!(pending.await, Err(BodyError::Cancelled)));
        }
        {
            let mut pending = pin!(writer.write_frame(BodyFrame::End, &never));
            assert!(poll_once(pending.as_mut()).await.is_none());
        }
        assert!(matches!(
            body.read(&never).await.unwrap(),
            BodyFrame::Data { end: false, .. }
        ));
        writer
            .write_frame(
                BodyFrame::Data {
                    data: vec![2],
                    end: true,
                },
                &never,
            )
            .await
            .unwrap();
        assert!(
            matches!(body.read(&never).await.unwrap(), BodyFrame::Data { data, end: true } if data == [2])
        );
    });
}

#[test]
fn queue_limits_and_precancellation_also_cover_headers() {
    block_on(async {
        let never = Cancellation::never();
        let (mut body, mut writer) = BodyReader::<Vec<u8>>::bounded(4, 2);
        let fields = Fields::try_from_pairs([("x", "1234")]).unwrap();
        assert!(matches!(
            writer
                .write_frame(BodyFrame::Headers { fields, end: true }, &never)
                .await,
            Err(BodyError::ChunkTooLarge {
                length: 5,
                limit: 4
            })
        ));
        writer.write(vec![1; 4], &never).await.unwrap();
        {
            let fields = Fields::try_from_pairs([("x", "1")]).unwrap();
            let mut pending =
                pin!(writer.write_frame(BodyFrame::Headers { fields, end: true }, &never));
            assert!(poll_once(pending.as_mut()).await.is_none());
        }
        assert!(matches!(
            body.read(&never).await.unwrap(),
            BodyFrame::Data { end: false, .. }
        ));
        let fields = Fields::try_from_pairs([("x", "1")]).unwrap();
        writer
            .write_frame(BodyFrame::Headers { fields, end: true }, &never)
            .await
            .unwrap();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(matches!(
            body.read(&cancelled).await,
            Err(BodyError::Cancelled)
        ));
        assert!(matches!(
            body.read(&never).await.unwrap(),
            BodyFrame::Headers { end: true, .. }
        ));
    });
}

#[test]
fn messages_deliver_owned_fields_without_mutating_the_initial_head() {
    block_on(async {
        let cancel = Cancellation::never();
        let (body, mut writer) = BodyReader::<Vec<u8>>::bounded(128, 1);
        let mut request = Request::new(Method::POST, "/", body);
        request.fields.add("x-initial", "initial").unwrap();
        let fields = Fields::try_from_pairs([("x-tail", "tail")]).unwrap();
        writer
            .write_frame(BodyFrame::Headers { fields, end: true }, &cancel)
            .await
            .unwrap();
        let BodyFrame::Headers { fields, end: true } = request.read(&cancel).await.unwrap() else {
            panic!("expected owned fields")
        };
        assert!(!request.fields.contains("x-tail"));
        assert_eq!(request.fields.len(), 1);
        let mut headers = base_http::HttpHeaders::new(request.fields.clone());
        headers.append_trailers(fields).unwrap();
        assert_eq!(headers.trailers()[0].value(), b"tail");
        assert_eq!(request.fields.len(), 1);
        let mut response = Response::new(Status::OK, request.body);
        assert!(matches!(
            response.read(&cancel).await.unwrap(),
            BodyFrame::End
        ));
    });
}

#[test]
fn mapping_data_preserves_completion_and_moves_header_fields() {
    let frame = BodyFrame::Data {
        data: Data(vec![1]),
        end: true,
    };
    assert!(
        matches!(frame.map_data(|data| data.0), BodyFrame::Data { data, end: true } if data == [1])
    );
    let frame: BodyFrame<Data> = BodyFrame::Headers {
        fields: Fields::new(),
        end: false,
    };
    let frame: BodyFrame<Vec<u8>> = frame.map_data(|_| panic!("must not map fields"));
    assert!(!frame.is_end());
    assert!(BodyFrame::<Data>::End.map_data(|_| ()).is_end());
}
