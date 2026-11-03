// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Body collection helpers and the [`readable_body_tests!`](crate::readable_body_tests) suite.
//!
//! The suite follows `protocol-http`'s `AReadableBody` shared behaviour and extends it with the rest of the [`ReadableBody`] contract. Every assertion is a standalone function taking a fresh [`Sample`], so an implementation which supports only part of the contract can call a subset.

use base_http::{
    Fields,
    body::{BodyFrame, ReadableBody},
};
use base_scheduler::Cancellation;

/// The frames of a body read to its end.
#[derive(Clone, Debug, Default)]
pub struct Collected {
    /// The concatenated data frames.
    pub data: Vec<u8>,
    /// The trailing header frame, if any.
    pub trailers: Option<Fields>,
    /// The number of data frames, including empty ones.
    pub chunks: usize,
}

/// Read `body` until a frame ends it, requiring that headers occur at most once and after all data.
pub async fn collect<B: ReadableBody>(
    body: &mut B,
    cancellation: &Cancellation,
) -> Result<Collected, B::Error> {
    let mut collected = Collected::default();
    loop {
        let frame = body.read(cancellation).await?;
        let end = frame.is_end();
        match frame {
            BodyFrame::Data { data, .. } => {
                assert!(
                    collected.trailers.is_none(),
                    "body data follows its trailers"
                );
                collected.data.extend_from_slice(data.as_ref());
                collected.chunks += 1;
            }
            BodyFrame::Headers { fields, .. } => {
                assert!(collected.trailers.is_none(), "body has repeated trailers");
                collected.trailers = Some(fields);
            }
            BodyFrame::End => {}
        }
        if end {
            return Ok(collected);
        }
    }
}

/// Read `body` to its end and return its data, panicking if a read fails.
pub async fn payload<B: ReadableBody>(body: &mut B) -> Vec<u8> {
    collect(body, &Cancellation::never())
        .await
        .expect("read the body")
        .data
}

/// A fresh body and the data reading it must produce.
#[derive(Debug)]
pub struct Sample<B> {
    /// A body that has not been read.
    pub body: B,
    /// The concatenated data the body produces.
    pub data: Vec<u8>,
    /// The trailer fields the body ends with, as `(name, value)` pairs, or `None` for no trailer frame.
    pub trailers: Option<Vec<(String, Vec<u8>)>>,
}

impl<B> Sample<B> {
    /// Pair a fresh body with its expected data and no trailers.
    pub fn new(body: B, data: impl Into<Vec<u8>>) -> Self {
        Self {
            body,
            data: data.into(),
            trailers: None,
        }
    }

    /// Expect the body to end with a trailer frame holding `fields`.
    pub fn with_trailers(mut self, fields: &Fields) -> Self {
        self.trailers = Some(pairs(fields));
        self
    }
}

fn pairs(fields: &Fields) -> Vec<(String, Vec<u8>)> {
    fields
        .pairs()
        .iter()
        .map(|field| (field.name().to_owned(), field.value().to_vec()))
        .collect()
}

/// After `close(None)`, a read returns `End`.
pub async fn read_after_close<B: ReadableBody>(mut sample: Sample<B>) {
    sample.body.close(None);
    let frame = sample.body.read(&Cancellation::never()).await;
    assert!(
        matches!(frame, Ok(BodyFrame::End)),
        "a read after close must return End"
    );
}

/// After `close(None)`, the body is empty.
pub async fn empty_after_close<B: ReadableBody>(mut sample: Sample<B>) {
    sample.body.close(None);
    assert!(sample.body.is_empty(), "a closed body must be empty");
}

/// Reading produces the expected data and trailers, with trailers only when the body says it may have them, and its length does not change.
pub async fn read_to_end<B: ReadableBody>(mut sample: Sample<B>) {
    let length = sample.body.length();
    let empty = sample.body.is_empty();
    let trailers = sample.body.may_have_trailers();
    let collected = collect(&mut sample.body, &Cancellation::never())
        .await
        .expect("read the body");
    assert_eq!(collected.data, sample.data, "body data");
    assert!(
        !empty || collected.chunks == 0,
        "an empty body produced data"
    );
    assert!(
        trailers || collected.trailers.is_none(),
        "a body without trailers produced them"
    );
    assert_eq!(
        collected.trailers.as_ref().map(pairs),
        sample.trailers,
        "body trailers"
    );
    assert_eq!(
        sample.body.length(),
        length,
        "body length changed while reading"
    );
}

/// After a frame ends the body, further reads keep returning `End`.
pub async fn end_is_terminal<B: ReadableBody>(mut sample: Sample<B>) {
    let never = Cancellation::never();
    collect(&mut sample.body, &never)
        .await
        .expect("read the body");
    for _ in 0..2 {
        let frame = sample.body.read(&never).await;
        assert!(
            matches!(frame, Ok(BodyFrame::End)),
            "a read after the end must return End"
        );
    }
}

/// A read with a cancelled token fails without consuming anything.
pub async fn cancelled_read_consumes_nothing<B: ReadableBody>(mut sample: Sample<B>) {
    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(
        sample.body.read(&cancelled).await.is_err(),
        "a cancelled read must fail"
    );
    let collected = collect(&mut sample.body, &Cancellation::never())
        .await
        .expect("read the body");
    assert_eq!(
        collected.data, sample.data,
        "a cancelled read consumed data"
    );
}

/// A rewindable body replays its data after a rewind; any other body refuses to rewind.
pub async fn rewind<B: ReadableBody>(mut sample: Sample<B>) {
    let never = Cancellation::never();
    let rewindable = sample.body.is_rewindable();
    collect(&mut sample.body, &never)
        .await
        .expect("read the body");
    assert_eq!(
        sample.body.rewind(),
        rewindable,
        "rewind must succeed exactly when the body is rewindable"
    );
    if rewindable {
        let replay = collect(&mut sample.body, &never)
            .await
            .expect("read the rewound body");
        assert_eq!(replay.data, sample.data, "rewound body data");
    }
}

/// Generate `#[test]` functions checking a [`ReadableBody`] implementation, mirroring `protocol-http`'s `AReadableBody`.
///
/// The runner receives an async closure taking a fresh [`Sample`]. It constructs the body and its expected data for each case, drives the future to completion, and owns any deadline. Invoke it inside a dedicated module:
///
/// ```
/// use base_http::body::BufferedBody;
/// use base_http_fixtures::body::Sample;
///
/// fn run(test: impl AsyncFnOnce(Sample<BufferedBody<Vec<u8>>>)) {
///     let body = BufferedBody::new([b"Hello".to_vec(), b" World".to_vec()]);
///     futures_lite::future::block_on(test(Sample::new(body, "Hello World")));
/// }
///
/// mod buffered {
///     base_http_fixtures::readable_body_tests!(run: super::run);
/// }
/// # fn main() {}
/// ```
#[macro_export]
macro_rules! readable_body_tests {
    (run: $run:path $(,)?) => {
        $crate::__readable_body_cases! { $run;
            read_after_close,
            empty_after_close,
            read_to_end,
            end_is_terminal,
            cancelled_read_consumes_nothing,
            rewind,
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __readable_body_cases {
    ($run:path; $($name:ident),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                $run(async |sample| $crate::body::$name(sample).await);
            }
        )+
    };
}
