#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Waker};

    fn sample() -> Publication {
        loop {
            if let Some(publication) = publication() { return publication; }
        }
    }

    async fn response(publication: Option<Publication>) {
        consumed(publication);
        read_ready();
        // Exercise the exact hazard: a response write suspends and another
        // connection consumes a read and finishes its response before it resumes.
        let mut suspended=false;
        poll_fn(|_| {
            if suspended { Poll::Ready(()) } else { suspended=true; Poll::Pending }
        }).await;
        response_written();
    }

    #[test]
    fn interleaved_response_writes_keep_samples_with_their_connection() {
        for second_sampled in [false,true] {
            STATE.with(|s| *s.borrow_mut()=State::default());
            collection_begin(2);
            let first=sample();
            let second=second_sampled.then(sample);
            collection_end();
            driver_return();
            let mut a=pin!(task(connection(response(Some(first)))));
            let mut b=pin!(task(connection(response(second))));
            let mut cx=Context::from_waker(Waker::noop());
            assert!(a.as_mut().poll(&mut cx).is_pending());
            assert!(b.as_mut().poll(&mut cx).is_pending());
            STATE.with(|s| assert_eq!(s.borrow().pending.len(),1+usize::from(second_sampled)));
            assert!(b.as_mut().poll(&mut cx).is_ready());
            STATE.with(|s| {
                let s=s.borrow();
                assert_eq!(s.pending.len(),1);
                assert!(s.pending.contains_key(&1));
            });
            assert!(a.as_mut().poll(&mut cx).is_ready());
            STATE.with(|s| assert!(s.borrow().pending.is_empty()));
        }
    }
}
