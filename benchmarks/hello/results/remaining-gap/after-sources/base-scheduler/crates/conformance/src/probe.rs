// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::Wake;

#[derive(Default)]
pub(crate) struct WakeProbe(pub(crate) AtomicUsize);
impl Wake for WakeProbe {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub(crate) struct DropProbe(pub(crate) Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
