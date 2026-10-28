// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{InterimError, InterimResponses, ResponseHead, Status};
use base_scheduler::Cancellation;
use futures_lite::future::{block_on, poll_once};
use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[test]
fn interim_heads_are_delivered_in_order_with_their_fields() {
    block_on(async {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&seen);
        let mut interim = InterimResponses::new(move |response: ResponseHead| {
            observed.lock().unwrap().push(response);
            async { Ok::<_, Infallible>(()) }
        });
        let mut hints = ResponseHead::new(Status::EARLY_HINTS);
        hints.fields.add("link", "</app.css>; rel=preload").unwrap();
        interim
            .send_interim_response(hints, &Cancellation::never())
            .await
            .unwrap();
        interim
            .send_interim_response(ResponseHead::new(Status::CONTINUE), &Cancellation::never())
            .await
            .unwrap();
        assert!(!interim.is_finished());
        interim.finish();
        assert!(interim.is_finished());
        let seen = seen.lock().unwrap();
        assert_eq!(
            seen.iter().map(|head| head.status).collect::<Vec<_>>(),
            [Status::EARLY_HINTS, Status::CONTINUE]
        );
        assert!(seen[0].fields.contains("link"));
    });
}

#[test]
fn final_statuses_including_101_are_never_sent_to_the_interim_hook() {
    block_on(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let mut interim = InterimResponses::new(move |_: ResponseHead| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, Infallible>(()) }
        });
        for status in [Status::SWITCHING_PROTOCOLS, Status::OK, Status::NOT_FOUND] {
            assert!(
                matches!(interim.send_interim_response(ResponseHead::new(status), &Cancellation::never()).await,
                Err(InterimError::InvalidStatus(found)) if found == status)
            );
            assert!(!interim.is_finished());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        interim
            .send_interim_response(ResponseHead::new(Status::CONTINUE), &Cancellation::never())
            .await
            .unwrap();
        interim.finish();
        assert!(matches!(
            interim
                .send_interim_response(
                    ResponseHead::new(Status::EARLY_HINTS),
                    &Cancellation::never()
                )
                .await,
            Err(InterimError::Finished)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn precancellation_preserves_the_hook_and_does_not_invoke_it() {
    block_on(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let mut interim = InterimResponses::new(move |_: ResponseHead| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, Infallible>(()) }
        });
        let cancellation = Cancellation::new();
        cancellation.cancel();
        assert!(matches!(
            interim
                .send_interim_response(ResponseHead::new(Status::CONTINUE), &cancellation)
                .await,
            Err(InterimError::Cancelled)
        ));
        assert!(!interim.is_finished());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn handler_failure_ends_interim_delivery_and_preserves_the_cause() {
    let mut interim = InterimResponses::new(|_: ResponseHead| async {
        Err::<(), _>(std::io::Error::other("write failed"))
    });
    let error = block_on(
        interim.send_interim_response(ResponseHead::new(Status::CONTINUE), &Cancellation::never()),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "write failed");
    assert!(interim.is_finished());
}

#[test]
fn abandoning_a_started_send_prevents_further_heads() {
    let mut interim =
        InterimResponses::new(|_: ResponseHead| std::future::pending::<Result<(), Infallible>>());
    let cancellation = Cancellation::never();
    {
        let mut send = std::pin::pin!(
            interim.send_interim_response(ResponseHead::new(Status::CONTINUE), &cancellation)
        );
        assert!(block_on(poll_once(send.as_mut())).is_none());
    }
    assert!(interim.is_finished());
}

#[test]
fn cancellation_ends_an_in_progress_send_even_if_the_observer_ignores_it() {
    let mut interim =
        InterimResponses::new(|_: ResponseHead| std::future::pending::<Result<(), Infallible>>());
    let cancellation = Cancellation::new();
    {
        let mut send = std::pin::pin!(
            interim.send_interim_response(ResponseHead::new(Status::CONTINUE), &cancellation)
        );
        assert!(block_on(poll_once(send.as_mut())).is_none());
        cancellation.cancel();
        assert!(matches!(block_on(send), Err(InterimError::Cancelled)));
    }
    assert!(interim.is_finished());
}

#[test]
fn unpolled_send_does_not_change_exchange_state_and_futures_are_send() {
    fn require_send<T: Send>(value: T) -> T {
        value
    }
    let mut interim = InterimResponses::new(|_: ResponseHead| async { Ok::<_, Infallible>(()) });
    drop(require_send(interim.send_interim_response(
        ResponseHead::new(Status::CONTINUE),
        &Cancellation::never(),
    )));
    assert!(!interim.is_finished());
}
