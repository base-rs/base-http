// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::sharded::WorkerTimers;

#[test]
fn shutdown_rejects_external_registration_and_cached_assistance() {
    let mut owner = WorkerTimers::with_capacity(4);
    let helper = owner.assistance();
    let now = Instant::now();
    let handle = owner.try_schedule(now, 5).unwrap();
    assert!(helper.publication.due(now)); // A helper already observed this hint.
    drop(owner);
    assert_eq!(helper.try_schedule(now, 6).err(), Some(6));
    assert_eq!(helper.collect(now, &mut Vec::with_capacity(1)), Some(0));
    assert_eq!(helper.next_deadline(), None);
    assert!(!handle.cancel());
    assert!(!handle.is_cancelled());
}
