// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{Cancellation, Clock, OperationError, OperationResult};
use futures_lite::future::block_on;
use std::sync::Mutex;
use std::time::{Duration, Instant};

// Logical time only: tests the default relative-wait contract, not a timer backend.
struct LogicalClock {
    now: Mutex<Instant>,
    deadlines: Mutex<Vec<Instant>>,
}

impl Clock for LogicalClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }

    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.deadlines.lock().unwrap().push(deadline);
        *self.now.lock().unwrap() = deadline;
        Ok(())
    }
}

#[test]
fn relative_sleep_is_lazy_and_uses_the_supplied_clock_domain() {
    let start = Instant::now();
    let clock = LogicalClock {
        now: Mutex::new(start),
        deadlines: Mutex::default(),
    };
    let token = Cancellation::never();
    let wait = clock.sleep(Duration::from_secs(3), &token);
    assert!(clock.deadlines.lock().unwrap().is_empty());
    *clock.now.lock().unwrap() = start + Duration::from_secs(10);
    block_on(wait).unwrap();
    let deadline = start + Duration::from_secs(13);
    assert_eq!(*clock.deadlines.lock().unwrap(), vec![deadline]);
    assert_eq!(clock.now(), deadline);

    block_on(clock.sleep(Duration::ZERO, &token)).unwrap();
    assert_eq!(*clock.deadlines.lock().unwrap(), vec![deadline, deadline]);
}

#[test]
fn cancellation_and_overflow_do_not_delegate_to_a_timer() {
    let clock = LogicalClock {
        now: Mutex::new(Instant::now()),
        deadlines: Mutex::default(),
    };
    let token = Cancellation::new();
    token.cancel();
    for duration in [Duration::ZERO, Duration::MAX] {
        assert!(matches!(
            block_on(clock.sleep(duration, &token)),
            Err(OperationError::Cancelled(_))
        ));
    }
    let error = block_on(clock.sleep(Duration::MAX, &Cancellation::never())).unwrap_err();
    assert!(
        matches!(error, OperationError::Io(error) if error.kind() == std::io::ErrorKind::InvalidInput)
    );
    assert!(clock.deadlines.lock().unwrap().is_empty());
}
