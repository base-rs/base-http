// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{IoBuf, IoBufMut, IoBufSequence, IoBufSequenceMut, OperationError};
use event_listener::Event;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// Lazily allocated retirement state shared by the two directions' owned operation paths.
#[derive(Default)]
pub(crate) struct OperationState {
    activity: Option<Arc<Activity>>,
}
struct Activity {
    running: AtomicBool,
    retired: Event,
}

// Field order releases the owner before publishing retirement, including during unwinding.
pub(crate) struct OperationGuard<T> {
    value: Option<T>,
    retirement: Retirement,
}
struct Retirement(Option<Arc<Activity>>);
impl Retirement {
    fn retire(&mut self) {
        if let Some(activity) = self.0.take() {
            activity.running.store(false, Ordering::Release);
            activity.retired.notify(usize::MAX);
        }
    }
}
impl Drop for Retirement {
    fn drop(&mut self) {
        self.retire();
    }
}
impl<T> OperationGuard<T> {
    pub(crate) fn retire(&mut self) {
        self.retirement.retire();
    }
    pub(crate) fn value_mut(&mut self) -> &mut T {
        self.value.as_mut().unwrap()
    }
    pub(crate) fn into_inner(mut self) -> T {
        let value = self.value.take().unwrap();
        drop(self);
        value
    }
}
impl<T: AsRef<[u8]>> AsRef<[u8]> for OperationGuard<T> {
    fn as_ref(&self) -> &[u8] {
        self.value.as_ref().unwrap().as_ref()
    }
}
impl<T: AsMut<[u8]>> AsMut<[u8]> for OperationGuard<T> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.value_mut().as_mut()
    }
}
// SAFETY: The guard retains the complete stable owner without changing its exposed allocation or range.
unsafe impl<T: IoBuf> IoBuf for OperationGuard<T> {}
// SAFETY: Only the guard owns mutable access to the retained buffer.
unsafe impl<T: IoBufMut> IoBufMut for OperationGuard<T> {}
// SAFETY: Every range and prepared I/O vector is forwarded unchanged to the exclusively retained owner.
unsafe impl<T: IoBufSequence> IoBufSequence for OperationGuard<T> {
    fn buffer_count(&self) -> usize {
        self.value.as_ref().unwrap().buffer_count()
    }
    fn buffer(&self, index: usize) -> &[u8] {
        self.value.as_ref().unwrap().buffer(index)
    }
    fn io_slices(&self) -> Option<&[std::io::IoSlice<'_>]> {
        self.value.as_ref().unwrap().io_slices()
    }
}
// SAFETY: The guard exclusively owns the underlying sequence and preserves its writable ranges.
unsafe impl<T: IoBufSequenceMut> IoBufSequenceMut for OperationGuard<T> {
    fn buffer_mut(&mut self, index: usize) -> &mut [u8] {
        self.value_mut().buffer_mut(index)
    }
    fn io_slices_mut(&mut self) -> Option<&[std::io::IoSliceMut<'_>]> {
        self.value_mut().io_slices_mut()
    }
}
impl OperationState {
    pub(crate) fn begin<T>(&mut self, value: T) -> Result<OperationGuard<T>, (OperationError, T)> {
        let activity = self.activity.get_or_insert_with(|| {
            let retired = Event::new();
            retired.notify(0);
            Arc::new(Activity {
                running: AtomicBool::new(false),
                retired,
            })
        });
        if activity.running.load(Ordering::Acquire) {
            return Err((io::Error::from(io::ErrorKind::WouldBlock).into(), value));
        }
        // This lane is borrowed exclusively; only the previous guard can change running, and only by clearing it. The acquire above observes its retirement before a new operation starts.
        activity.running.store(true, Ordering::Relaxed);
        Ok(OperationGuard {
            value: Some(value),
            retirement: Retirement(Some(Arc::clone(activity))),
        })
    }
    pub(crate) async fn close(&mut self) {
        if let Some(activity) = &self.activity {
            loop {
                event_listener::listener!(activity.retired => listener);
                if !activity.running.load(Ordering::Acquire) {
                    break;
                }
                listener.await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
