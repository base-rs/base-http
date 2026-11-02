// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The shared `ReadableBody` contract, applied to each body type this crate provides.
use base_http::{
    Fields,
    body::{BodyFrame, BodyReader, BufferedBody, EmptyBody, HeadBody},
};
use base_http_fixtures::body::Sample;
use base_scheduler::Cancellation;
use futures_lite::future::block_on;

fn empty(test: impl AsyncFnOnce(Sample<EmptyBody>)) {
    block_on(test(Sample::new(EmptyBody, "")));
}

mod empty {
    base_http_fixtures::readable_body_tests!(run: super::empty);
}

fn buffered(test: impl AsyncFnOnce(Sample<BufferedBody<Vec<u8>>>)) {
    let body = BufferedBody::new([b"Hello".to_vec(), Vec::new(), b" World".to_vec()]);
    block_on(test(Sample::new(body, "Hello World")));
}

mod buffered {
    base_http_fixtures::readable_body_tests!(run: super::buffered);
}

fn head(test: impl AsyncFnOnce(Sample<HeadBody>)) {
    block_on(test(Sample::new(HeadBody::new(Some(11)), "")));
}

mod head {
    base_http_fixtures::readable_body_tests!(run: super::head);
}

// A reader whose writer queued data and trailers, then finished.
fn reader(test: impl AsyncFnOnce(Sample<BodyReader<Vec<u8>>>)) {
    block_on(async {
        let never = Cancellation::never();
        let (body, mut writer) = BodyReader::with_length(64, 8, 11);
        writer.write(b"Hello".to_vec(), &never).await.unwrap();
        writer.write(b" World".to_vec(), &never).await.unwrap();
        let mut fields = Fields::new();
        fields.add("etag", "abcd").unwrap();
        writer
            .write_frame(BodyFrame::Headers { fields, end: true }, &never)
            .await
            .unwrap();
        test(Sample::new(body, "Hello World")).await;
    });
}

mod reader {
    base_http_fixtures::readable_body_tests!(run: super::reader);
}
