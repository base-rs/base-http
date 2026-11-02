// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The `ReadableBody` suite passes for the fixture bodies and fails for bodies which break each part of the contract.
use base_http::{
    Fields,
    body::{BodyError, BodyFrame, BufferedBody, EmptyBody, ReadableBody},
};
use base_http_fixtures::{
    Latch,
    body::{self, Sample},
    protocol::{LARGE_BODY, Reply, Staged, Step},
};
use base_scheduler::Cancellation;
use futures_lite::future::{block_on, poll_once};
use std::pin::pin;

fn etag() -> Fields {
    let mut fields = Fields::new();
    fields.add("etag", "abcd").unwrap();
    fields
}

fn staged(test: impl AsyncFnOnce(Sample<Reply<EmptyBody>>)) {
    let released = Latch::new();
    released.release();
    let body = Staged::new(
        [
            Step::Data(b"Hello".to_vec()),
            Step::Wait(released),
            Step::Data(b" World".to_vec()),
            Step::Trailers(etag()),
        ],
        Some(11),
    );
    block_on(test(
        Sample::new(Reply::Staged(body), "Hello World").with_trailers(&etag()),
    ));
}

mod staged {
    base_http_fixtures::readable_body_tests!(run: super::staged);
}

fn buffered(test: impl AsyncFnOnce(Sample<Reply<EmptyBody>>)) {
    let body = BufferedBody::new([b"Hello".to_vec(), b" World".to_vec()]);
    block_on(test(Sample::new(Reply::Buffered(body), "Hello World")));
}

mod buffered {
    base_http_fixtures::readable_body_tests!(run: super::buffered);
}

fn echo(test: impl AsyncFnOnce(Sample<Reply<BufferedBody<&'static [u8]>>>)) {
    let body = BufferedBody::new([b"Hello".as_slice(), b" World"]);
    block_on(test(Sample::new(Reply::Echo(body), "Hello World")));
}

mod echo {
    base_http_fixtures::readable_body_tests!(run: super::echo);
}

fn zeroes(test: impl AsyncFnOnce(Sample<Reply<EmptyBody>>)) {
    let body = Reply::Zeroes {
        remaining: LARGE_BODY,
    };
    block_on(test(Sample::new(body, vec![0; LARGE_BODY as usize])));
}

mod zeroes {
    base_http_fixtures::readable_body_tests!(run: super::zeroes);
}

#[test]
fn a_staged_wait_holds_until_released_and_observes_cancellation() {
    block_on(async {
        let latch = Latch::new();
        let mut body = Staged::new(
            [Step::Wait(latch.clone()), Step::Data(b"after".to_vec())],
            None,
        );
        assert!(!body.may_have_trailers());
        assert!(!body.is_empty());
        assert!(Staged::new([Step::Wait(Latch::new())], None).is_empty());
        let cancellation = Cancellation::new();
        {
            let mut read = pin!(body.read(&cancellation));
            assert!(poll_once(read.as_mut()).await.is_none());
            cancellation.cancel();
            assert!(read.await.is_err());
        }
        latch.release();
        let collected = body::collect(&mut body, &Cancellation::never())
            .await
            .unwrap();
        assert_eq!(collected.data, b"after");
    });
}

#[test]
fn a_reply_reports_readiness_and_trailers_by_variant() {
    let zeroes = Reply::<EmptyBody>::Zeroes { remaining: 1 };
    assert!(zeroes.is_ready() && !zeroes.is_empty() && !zeroes.may_have_trailers());
    let staged = Reply::<EmptyBody>::Staged(Staged::new([Step::Trailers(etag())], None));
    assert!(!staged.is_ready() && staged.may_have_trailers());
    let echo = Reply::Echo(EmptyBody);
    assert!(echo.is_ready() && echo.is_empty() && !echo.may_have_trailers());
    let buffered = Reply::<EmptyBody>::Buffered(BufferedBody::default());
    assert!(buffered.is_ready());
}

/// A body which breaks one part of the `ReadableBody` contract.
#[derive(Clone, Copy, Debug)]
enum Fault {
    // Keep producing data after a close.
    DataAfterClose,
    // Claim to be non-empty after a close.
    FullAfterClose,
    // Produce different data than expected.
    WrongData,
    // Produce data while claiming to be empty.
    DataWhenEmpty,
    // Produce trailers without admitting it may.
    UndeclaredTrailers,
    // Change the reported length while reading.
    ChangingLength,
    // Produce data after the end.
    DataAfterEnd,
    // Ignore cancellation.
    IgnoresCancellation,
    // Consume a frame when a read is cancelled.
    CancellationConsumes,
    // Claim to be rewindable but refuse.
    RefusesRewind,
    // Rewind, but replay nothing.
    ForgetfulRewind,
    // Produce data after the trailers.
    DataAfterTrailers,
    // Produce trailers twice.
    RepeatedTrailers,
    // Omit the expected trailers.
    DroppedTrailers,
}

#[derive(Debug)]
struct Faulty {
    fault: Fault,
    frames: Vec<BodyFrame<Vec<u8>>>,
    closed: bool,
    rewound: bool,
    ended: bool,
}

impl Faulty {
    fn sample(fault: Fault) -> Sample<Self> {
        let data = |bytes: &[u8]| BodyFrame::Data {
            data: bytes.to_vec(),
            end: false,
        };
        let headers = |end| BodyFrame::Headers {
            fields: etag(),
            end,
        };
        let frames = match fault {
            Fault::WrongData => vec![data(b"wrong")],
            Fault::UndeclaredTrailers => vec![data(b"data"), headers(true)],
            Fault::DataAfterTrailers => vec![headers(false), data(b"data")],
            Fault::RepeatedTrailers => vec![data(b"data"), headers(false), headers(true)],
            _ => vec![data(b"data")],
        };
        let mut frames = frames;
        frames.reverse();
        let sample = Sample::new(
            Self {
                fault,
                frames,
                closed: false,
                rewound: false,
                ended: false,
            },
            "data",
        );
        match fault {
            Fault::DroppedTrailers => sample.with_trailers(&etag()),
            _ => sample,
        }
    }

    fn is(&self, fault: Fault) -> bool {
        std::mem::discriminant(&self.fault) == std::mem::discriminant(&fault)
    }
}

impl ReadableBody for Faulty {
    type Data = Vec<u8>;
    type Error = BodyError;

    fn is_empty(&self) -> bool {
        if self.closed {
            return !self.is(Fault::FullAfterClose);
        }
        self.is(Fault::DataWhenEmpty)
    }

    fn length(&self) -> Option<u64> {
        (self.is(Fault::ChangingLength) && self.frames.is_empty()).then_some(1)
    }

    fn may_have_trailers(&self) -> bool {
        !self.is(Fault::UndeclaredTrailers)
    }

    fn is_rewindable(&self) -> bool {
        self.is(Fault::RefusesRewind) || self.is(Fault::ForgetfulRewind)
    }

    fn rewind(&mut self) -> bool {
        self.rewound = true;
        self.is(Fault::ForgetfulRewind)
    }

    fn close(&mut self, _: Option<BodyError>) {
        self.closed = true;
        if !self.is(Fault::DataAfterClose) {
            self.frames.clear();
        }
    }

    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, BodyError> {
        if cancellation.check().is_err() && !self.is(Fault::IgnoresCancellation) {
            if self.is(Fault::CancellationConsumes) {
                self.frames.pop();
            }
            return Err(BodyError::Closed);
        }
        if self.rewound {
            return Ok(BodyFrame::End);
        }
        Ok(match self.frames.pop() {
            Some(frame) => frame,
            None if self.ended && self.is(Fault::DataAfterEnd) => BodyFrame::Data {
                data: b"more".to_vec(),
                end: false,
            },
            None => {
                self.ended = true;
                BodyFrame::End
            }
        })
    }
}

macro_rules! faults {
    ($($name:ident: $fault:ident => $assertion:ident, $message:literal;)+) => {
        $(
            #[test]
            #[should_panic(expected = $message)]
            fn $name() {
                block_on(body::$assertion(Faulty::sample(Fault::$fault)));
            }
        )+
    };
}

faults! {
    data_after_close_fails: DataAfterClose => read_after_close, "a read after close must return End";
    full_after_close_fails: FullAfterClose => empty_after_close, "a closed body must be empty";
    wrong_data_fails: WrongData => read_to_end, "body data";
    data_when_empty_fails: DataWhenEmpty => read_to_end, "an empty body produced data";
    undeclared_trailers_fail: UndeclaredTrailers => read_to_end, "a body without trailers produced them";
    changing_length_fails: ChangingLength => read_to_end, "body length changed while reading";
    data_after_end_fails: DataAfterEnd => end_is_terminal, "a read after the end must return End";
    ignored_cancellation_fails: IgnoresCancellation => cancelled_read_consumes_nothing, "a cancelled read must fail";
    consuming_cancellation_fails: CancellationConsumes => cancelled_read_consumes_nothing, "a cancelled read consumed data";
    refused_rewind_fails: RefusesRewind => rewind, "rewind must succeed exactly when the body is rewindable";
    forgetful_rewind_fails: ForgetfulRewind => rewind, "rewound body data";
    data_after_trailers_fails: DataAfterTrailers => read_to_end, "body data follows its trailers";
    repeated_trailers_fail: RepeatedTrailers => read_to_end, "body has repeated trailers";
    dropped_trailers_fail: DroppedTrailers => read_to_end, "body trailers";
}

#[test]
fn a_faulty_body_passes_the_assertions_it_does_not_break() {
    block_on(async {
        body::read_after_close(Faulty::sample(Fault::WrongData)).await;
        body::rewind(Faulty::sample(Fault::WrongData)).await;
    });
}
