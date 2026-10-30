// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::body::{BodyFrame, BufferedBody, EmptyBody, HeadBody, ReadableBody};
use base_scheduler::{Cancellation, Cancelled};
use futures_lite::future::block_on;

// A minimal body that relies on every provided observation.
#[derive(Default)]
struct Minimal {
    reads: usize,
    closed: Option<Option<Cancelled>>,
}

impl ReadableBody for Minimal {
    type Data = Vec<u8>;
    type Error = Cancelled;

    fn close(&mut self, error: Option<Cancelled>) {
        self.closed = Some(error);
    }

    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Cancelled> {
        cancellation.check()?;
        self.reads += 1;
        Ok(BodyFrame::End)
    }
}

#[test]
fn provided_observations_are_conservative() {
    let mut body = Minimal::default();
    assert!(!body.is_ready());
    assert!(!body.is_empty());
    assert_eq!(body.length(), None);
    assert!(body.may_have_trailers());
    assert!(!body.is_rewindable());
    assert!(!body.rewind());
    assert!(matches!(
        block_on(body.read(&Cancellation::never())),
        Ok(BodyFrame::End)
    ));
    assert_eq!(body.reads, 1);
}

// Generic code receives a `&mut` borrow as its own body type.
async fn read_rewind_and_close<B: ReadableBody>(
    mut body: B,
    cancellation: &Cancellation,
    error: Option<B::Error>,
) -> (bool, Result<BodyFrame<B::Data>, B::Error>, bool) {
    let rewindable = body.is_rewindable();
    let frame = body.read(cancellation).await;
    let rewound = body.rewind();
    body.close(error);
    (rewindable, frame, rewound)
}

#[test]
fn borrowed_bodies_forward_reads_rewinding_and_closure() {
    block_on(async {
        let never = Cancellation::never();
        let mut buffered = BufferedBody::new([b"one".to_vec(), b"two".to_vec()]);
        let (rewindable, frame, rewound) = read_rewind_and_close(&mut buffered, &never, None).await;
        assert!(rewindable && rewound);
        assert!(matches!(frame, Ok(BodyFrame::Data { data, end: false }) if data == b"one"));
        assert!(buffered.is_empty());
        assert!(buffered.rewind());
        assert_eq!(buffered.read_chunk(), Some(b"one".to_vec()));

        let mut minimal = Minimal::default();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let (rewindable, frame, rewound) =
            read_rewind_and_close(&mut minimal, &cancelled, Some(Cancelled)).await;
        assert!(!rewindable && !rewound);
        assert!(matches!(frame, Err(Cancelled)));
        assert_eq!(minimal.reads, 0);
        assert!(matches!(minimal.closed, Some(Some(Cancelled))));
    });
}

#[test]
fn static_bodies_reject_precancelled_reads() {
    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(matches!(
        block_on(EmptyBody.read(&cancelled)),
        Err(Cancelled)
    ));
    let mut head = HeadBody::new(Some(5));
    assert!(matches!(block_on(head.read(&cancelled)), Err(Cancelled)));
    assert!(matches!(
        block_on(head.read(&Cancellation::never())),
        Ok(BodyFrame::End)
    ));
}

#[test]
fn closing_a_head_body_preserves_its_length_and_completion() {
    let mut head = HeadBody::new(Some(5));
    head.close(Some(Cancelled));
    assert_eq!(head.length(), Some(5));
    assert!(head.is_empty());
    assert!(matches!(
        block_on(head.read(&Cancellation::never())),
        Ok(BodyFrame::End)
    ));
}
