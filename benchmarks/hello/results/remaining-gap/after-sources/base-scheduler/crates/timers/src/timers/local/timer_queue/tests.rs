// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::time::Duration;

#[test]
fn bounded_registration_expiry_updates_and_release_reuse_slots() {
    let now = Instant::now();
    let mut empty = TimerQueue::with_capacity(0);
    assert!(empty.is_empty());
    assert_eq!(empty.capacity(), 0);
    assert_eq!(empty.try_schedule(now, 42), Err(42));
    assert_eq!(empty.pop(), None);

    let mut queue = TimerQueue::with_capacity(1);
    let capacity = queue.capacity();
    let keys: Vec<_> = (0..capacity)
        .map(|value| queue.try_schedule(now, value).unwrap())
        .collect();
    assert_eq!(queue.len(), capacity);
    assert_eq!(queue.try_schedule(now, capacity), Err(capacity));
    assert_eq!(queue.pop_expired(now - Duration::from_secs(1)), None);
    assert_eq!(queue.update(keys[0], capacity + 1), Ok(0));
    assert!(queue.pop_expired(now).is_some());
    while queue.pop().is_some() {}
    assert_eq!(queue.len(), 0);
    // Collected values do not release their outstanding keys' reserved slots.
    assert_eq!(queue.try_schedule(now, 99), Err(99));
    assert_eq!(queue.update(keys[0], 100), Err(100));
    for key in &keys {
        assert_eq!(queue.release(*key), None);
    }
    let replacement = queue.try_schedule(now, 123).unwrap();
    for key in &keys {
        assert_eq!(queue.update(*key, 456), Err(456));
        assert_eq!(queue.release(*key), None);
    }
    assert_eq!(queue.release(replacement), Some(123));
    assert_eq!(queue.update(replacement, 789), Err(789));
    assert_eq!(queue.release(replacement), None);
    assert!(queue.next_deadline().is_none());
}

#[test]
fn full_queue_reclaims_a_cancelled_non_root_heap_entry() {
    let now = Instant::now();
    let mut queue = TimerQueue::with_capacity(8);
    let keys: Vec<_> = (0..queue.capacity())
        .map(|index| {
            queue
                .try_schedule(now + Duration::from_secs(index as u64 + 1), index)
                .unwrap()
        })
        .collect();
    assert_eq!(queue.next_deadline(), Some(now + Duration::from_secs(1)));
    assert_eq!(queue.release(keys[1]), Some(1));
    let replacement = queue
        .try_schedule(now + Duration::from_secs(100), 99)
        .unwrap();
    assert_ne!(replacement, keys[1]);
    assert_eq!(queue.update(keys[1], 100), Err(100));
    assert_eq!(queue.next_deadline(), Some(now + Duration::from_secs(1)));
    assert_eq!(queue.pop_expired(now), None);
    assert_eq!(queue.pop_expired(now + Duration::from_secs(1)), Some(0));
    for key in keys {
        queue.release(key);
    }
    assert_eq!(queue.release(replacement), Some(99));
    assert_eq!(queue.pop_expired(now + Duration::from_secs(200)), None);
}
