// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferLease, Cancellation, Cancelled};
use event_listener::Event;
#[cfg(not(test))]
use std::process::abort;
use std::{
    cell::UnsafeCell,
    future::{Future, poll_fn},
    io,
    pin::pin,
    ptr::NonNull,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};
#[cfg(test)]
use tests::abort;

/// A bounded, shareable collection of stable initialized slots in one contiguous slab. Construction reserves all payload storage; acquiring, freezing, slicing, and returning leases allocate no payload or reference-count wrappers.
///
/// Pools may be shared across connections and file pipelines. Exhaustion waits with cancellation instead of growing the pool. Retained views count against capacity. This portable implementation of `BufferPool` uses a heap allocation. Backends may supply another pool implementation with the same ownership contract.
#[derive(Clone)]
pub struct HeapBufferPool(pub(crate) Arc<Pool>);

pub(crate) struct Slot {
    pub(crate) readers: AtomicUsize,
}

pub(crate) struct Pool {
    slab: Arc<Slab>,
    waiters: Mutex<usize>,
}

// Availability and lifetime transitions share one atomic word, so closure cannot miss a concurrent return and exhaustion cannot miss a wakeup.
const CLOSED: usize = 1 << (usize::BITS - 1);
const WAITING: usize = CLOSED >> 1;
const AVAILABLE: usize = WAITING - 1;
const WIDTH: usize = usize::BITS as usize - 2;

// Unit tests reach the ownership limit through real acquisitions and clones. Production keeps the same conservative ceiling as Arc's reference count.
#[cfg(not(test))]
const COUNT_LIMIT: usize = isize::MAX as usize;
#[cfg(test)]
const COUNT_LIMIT: usize = 128;

// Each physical slot has one persistent strong ownership claim. Its availability bit, mutable lease, or complete group of frozen views owns that claim. Pool handles retain a separate owner while acquisition is possible. Closing the pool retires idle claims; outstanding leases retire theirs when returned.
pub(crate) struct Slab {
    pub(crate) slots: Box<[Slot]>,
    bytes: Box<[UnsafeCell<u8>]>,
    offset: usize,
    stride: usize,
    // One bit per stable slot; the final word never advertises unused indices.
    free: Box<[AtomicUsize]>,
    returned: Event,
    size: usize,
    alignment: usize,
}

// SAFETY: A set availability bit, one mutable lease, or immutable views counted in readers exclusively owns each slot. Only the mutable lease can mutate bytes. Clearing an availability bit acquires the final owner's release before accessing that slot. Slots and the byte slab never move or resize.
unsafe impl Sync for Slab {}

impl HeapBufferPool {
    pub fn new(buffers: usize, buffer_size: usize) -> io::Result<Self> {
        Self::with_options(crate::BufferPoolOptions::new(buffers, buffer_size))
    }

    /// Reserve one contiguous initialized allocation. Slot starts satisfy the requested alignment; padding is outside the usable payload budget.
    pub fn with_options(options: crate::BufferPoolOptions) -> io::Result<Self> {
        options.validate()?;
        let crate::BufferPoolOptions {
            buffers,
            buffer_size,
            alignment,
        } = options;
        let stride = (buffer_size + alignment - 1) & !(alignment - 1);
        let allocation_size = (buffers - 1) * stride + buffer_size + alignment - 1;
        let bytes = vec![0u8; allocation_size].into_boxed_slice();
        let offset = (bytes.as_ptr() as usize).wrapping_neg() & (alignment - 1);
        // SAFETY: UnsafeCell<u8> has exactly u8's layout and drop behavior. The allocation remains owned, initialized, and fixed in size; references are created only for individually owned slots.
        let bytes = unsafe { Box::from_raw(Box::into_raw(bytes) as *mut [UnsafeCell<u8>]) };
        let returned = Event::new();
        returned.notify(0);
        let free = (0..buffers.div_ceil(WIDTH))
            .map(|word| {
                let bits = (buffers - word * WIDTH).min(WIDTH);
                AtomicUsize::new((1 << bits) - 1)
            })
            .collect();
        let slab = Arc::new(Slab {
            slots: (0..buffers)
                .map(|_| Slot {
                    readers: AtomicUsize::new(0),
                })
                .collect(),
            bytes,
            offset,
            stride,
            free,
            returned,
            size: buffer_size,
            alignment,
        });
        // Mint one ownership claim per physical slot. Checkout, freeze, and return transfer it without updating the slab reference count.
        for _ in 0..buffers {
            let _ = Arc::into_raw(Arc::clone(&slab));
        }
        Ok(Self(Arc::new(Pool {
            slab,
            waiters: Mutex::new(0),
        })))
    }

    pub fn capacity(&self) -> usize {
        self.0.slab.slots.len()
    }
    pub fn buffer_size(&self) -> usize {
        self.0.slab.size
    }

    pub fn alignment(&self) -> usize {
        self.0.slab.alignment
    }

    /// Acquire exclusive writable storage immediately, or return `None` without allocating.
    pub fn try_acquire(&self) -> Option<BufferLease> {
        for (word_index, word) in self.0.slab.free.iter().enumerate() {
            let mut available = word.load(Ordering::Relaxed);
            while available & AVAILABLE != 0 {
                let bit = 1 << (available & AVAILABLE).trailing_zeros();
                match word.compare_exchange_weak(
                    available,
                    available & !bit,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let index = word_index * WIDTH + bit.trailing_zeros() as usize;
                        // SAFETY: Clearing the bit transfers its persistent slot claim to this lease.
                        let slab =
                            unsafe { NonNull::new_unchecked(Arc::as_ptr(&self.0.slab).cast_mut()) };
                        return Some(BufferLease::new(slab, index, self.0.slab.size));
                    }
                    Err(current) => available = current,
                }
            }
        }
        None
    }

    /// Wait for one lease. Pre-cancellation leaves capacity unchanged; a successfully acquired lease wins a later cancellation race.
    pub async fn acquire(&self, cancellation: &Cancellation) -> Result<BufferLease, Cancelled> {
        cancellation.check()?;
        if let Some(buffer) = self.try_acquire() {
            return Ok(buffer);
        }
        let mut cancelled = pin!(cancellation.cancelled());
        let _waiting = PoolWaiter::new(&self.0);
        loop {
            event_listener::listener!(self.0.slab.returned => listener);
            if let Some(buffer) = self.try_acquire() {
                return Ok(buffer);
            }
            let mut listener = pin!(listener);
            poll_fn(|context| {
                if cancelled.as_mut().poll(context).is_ready() {
                    return Poll::Ready(Err(Cancelled));
                }
                listener.as_mut().poll(context).map(|_| Ok(()))
            })
            .await?;
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // Closing and publishing a return share the same atomic word. Every captured availability bit transfers its claim to this drain; subsequent returns retire their own claim. The pool retains its separate slab owner throughout the drain.
        for word in &self.slab.free {
            let mut available = word.swap(CLOSED, Ordering::AcqRel) & AVAILABLE;
            while available != 0 {
                available &= available - 1;
                // SAFETY: Each captured bit owns exactly one claim minted at setup.
                unsafe { Arc::decrement_strong_count(Arc::as_ptr(&self.slab)) };
            }
        }
    }
}

struct PoolWaiter<'a>(&'a Pool);

impl<'a> PoolWaiter<'a> {
    fn new(pool: &'a Pool) -> Self {
        let mut waiters = pool.waiters.lock().unwrap();
        if *waiters == 0 {
            for word in &pool.slab.free {
                word.fetch_or(WAITING, Ordering::Release);
            }
        }
        if *waiters >= COUNT_LIMIT {
            abort();
        }
        *waiters += 1;
        Self(pool)
    }
}

impl Drop for PoolWaiter<'_> {
    fn drop(&mut self) {
        let mut waiters = self.0.waiters.lock().unwrap();
        *waiters -= 1;
        if *waiters == 0 {
            for word in &self.0.slab.free {
                word.fetch_and(!WAITING, Ordering::Release);
            }
        }
    }
}

impl Slab {
    pub(crate) fn pointer(&self, index: usize) -> *mut u8 {
        assert!(index < self.slots.len());
        // SAFETY: Every slot's complete payload fits in this stable allocation. UnsafeCell permits mutation of the individually leased range without forming a mutable reference covering other slots.
        unsafe {
            self.bytes
                .as_ptr()
                .cast::<u8>()
                .cast_mut()
                .add(self.offset + index * self.stride)
        }
    }

    /// Consume a slot claim. After publication, the pool may close and reclaim it immediately.
    ///
    /// SAFETY: `slab` must represent the unique mutable lease or final frozen reader's persistent slot claim.
    pub(crate) unsafe fn release(slab: NonNull<Self>, index: usize) {
        // SAFETY: The caller owns this slot's claim until it is published or retired below.
        let word = unsafe { &slab.as_ref().free[index / WIDTH] };
        let bit = 1 << (index % WIDTH);
        let mut state = word.load(Ordering::Relaxed);
        let mut notification = None;
        loop {
            if state & CLOSED != 0 {
                // SAFETY: A return after closure retires its claim instead of publishing it.
                unsafe { Arc::decrement_strong_count(slab.as_ptr()) };
                return;
            }
            if state & WAITING != 0 && notification.is_none() {
                // SAFETY: The slot claim is still ours. A temporary owner protects notification after publication, including concurrent pool closure. This reference-count update is confined to exhausted pools.
                unsafe {
                    Arc::increment_strong_count(slab.as_ptr());
                    notification = Some(Arc::from_raw(slab.as_ptr()));
                }
            }
            match word.compare_exchange_weak(
                state,
                state | bit,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => state = current,
            }
        }
        // Do not access `slab` or `word` after publication without a separate owner.
        if let Some(slab) = notification {
            slab.returned.notify_additional(1);
        }
    }

    pub(crate) fn retain(&self, index: usize) {
        // Frozen views share their slot's one slab claim, so guard their own count.
        if self.slots[index].readers.fetch_add(1, Ordering::Relaxed) >= COUNT_LIMIT {
            abort();
        }
    }

    pub(crate) unsafe fn release_reader(slab: NonNull<Self>, index: usize) {
        // SAFETY: A frozen reader retains its slot claim through the final decrement.
        if unsafe {
            slab.as_ref().slots[index]
                .readers
                .fetch_sub(1, Ordering::AcqRel)
        } == 1
        {
            // SAFETY: The final frozen reader transfers the unique slot claim.
            unsafe { Self::release(slab, index) };
        }
    }
}

impl crate::BufferPool for HeapBufferPool {
    type Buffer = BufferLease;
    fn capacity(&self) -> usize {
        HeapBufferPool::capacity(self)
    }
    fn buffer_size(&self) -> usize {
        HeapBufferPool::buffer_size(self)
    }
    fn alignment(&self) -> usize {
        HeapBufferPool::alignment(self)
    }
    fn try_acquire(&self) -> Option<Self::Buffer> {
        HeapBufferPool::try_acquire(self)
    }
    async fn acquire(&self, cancellation: &Cancellation) -> Result<Self::Buffer, Cancelled> {
        HeapBufferPool::acquire(self, cancellation).await
    }
}

#[cfg(test)]
mod tests;
