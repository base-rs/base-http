// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    ptr::NonNull,
    sync::atomic::AtomicU16,
};

pub(crate) struct Group {
    pub(crate) identifier: u16,
    groups: Arc<Mutex<Vec<bool>>>,
}
impl Group {
    pub(crate) fn new(controller: &Controller) -> io::Result<Self> {
        let mut groups = controller.buffer_groups.lock().unwrap();
        let identifier = if let Some(index) = groups.iter().position(|used| !used) {
            groups[index] = true;
            index
        } else {
            if groups.len() > u16::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "all provided-buffer groups are in use",
                ));
            }
            groups.push(true);
            groups.len() - 1
        };
        Ok(Self {
            identifier: identifier as u16,
            groups: Arc::clone(&controller.buffer_groups),
        })
    }
}
impl Drop for Group {
    fn drop(&mut self) {
        self.groups.lock().unwrap()[self.identifier as usize] = false;
    }
}

pub(crate) struct Pool {
    pub(crate) identifier: u64,
    pub(crate) controller: Weak<Controller>,
    group: Group,
    ring: NonNull<types::BufRingEntry>,
    layout: Layout,
    buffers: Box<[UnsafeCell<u8>]>,
    count: u16,
    entries: u16,
    buffer_size: usize,
    available: AtomicUsize,
    rearm: AtomicBool,
}
// SAFETY: Allocations stay fixed. Only the ring owner publishes entries and returns buffers; the kernel alone consumes them. A CQE gives one unique lease access to a completed buffer until that lease is dropped. Other threads only enqueue recycle commands.
unsafe impl Send for Pool {}
unsafe impl Sync for Pool {}

impl Pool {
    pub(super) fn new(
        controller: &Arc<Controller>,
        options: RecvMsgMultishotOptions,
        identifier: u64,
    ) -> io::Result<Arc<Self>> {
        options.validate()?;
        let entries = options.buffers.next_power_of_two();
        let overhead = 16 + std::mem::size_of::<libc::sockaddr_storage>();
        let buffer_size = options.buffer_size.saturating_add(overhead);
        let length = buffer_size.saturating_mul(options.buffers as usize);
        if buffer_size > i32::MAX as usize || length > isize::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "multishot requires metadata and payload sizes fitting the kernel buffer limits",
            ));
        }
        let group = Group::new(controller)?;
        // SAFETY: _SC_PAGESIZE is a supported, nonnegative constant on Linux.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let layout = Layout::from_size_align(
            entries as usize * std::mem::size_of::<types::BufRingEntry>(),
            page,
        )
        .expect("Linux page size is a power of two and the ring has at most 32768 entries");
        let ring = syscall("allocate-buffer-ring", || {
            // SAFETY: Layout is nonzero and valid; the allocation is retained through deregistration.
            let pointer = unsafe { alloc_zeroed(layout) };
            NonNull::new(pointer.cast()).ok_or(io::ErrorKind::OutOfMemory.into())
        })?;
        let pool = Arc::new(Self {
            identifier,
            controller: Arc::downgrade(controller),
            group,
            ring,
            layout,
            buffers: (0..length).map(|_| UnsafeCell::new(0)).collect(),
            count: options.buffers,
            entries,
            buffer_size,
            available: AtomicUsize::new(0),
            rearm: AtomicBool::new(false),
        });
        for identifier in 0..options.buffers {
            pool.recycle(identifier);
        }
        Ok(pool)
    }
    pub(crate) fn group(&self) -> u16 {
        self.group.identifier
    }
    pub(super) fn register(&self, ring: &io_uring::Submitter<'_>) -> io::Result<()> {
        syscall("register-buffer-ring", || {
            // SAFETY: The request retains this page-aligned ring and all payloads until original completion and deregistration. Only its exclusive ring owner publishes entries.
            unsafe {
                ring.register_buf_ring_with_flags(
                    self.ring.as_ptr() as u64,
                    self.entries,
                    self.group(),
                    0,
                )
            }
        })
    }
    pub(super) fn unregister(&self, ring: &io_uring::Submitter<'_>) -> io::Result<()> {
        syscall("unregister-buffer-ring", || {
            ring.unregister_buf_ring(self.group())
        })
    }
    pub(super) fn available(&self) -> usize {
        self.available.load(Ordering::Relaxed)
    }
    pub(super) fn selected(&self) {
        self.available.fetch_sub(1, Ordering::Relaxed);
    }
    pub(crate) fn needs_rearm(&self) -> bool {
        self.rearm.load(Ordering::Relaxed)
    }
    pub(super) fn set_rearm(&self, rearm: bool) {
        self.rearm.store(rearm, Ordering::Relaxed);
    }

    /// Called before registration, or exclusively by the original ring owner.
    pub(crate) fn recycle(&self, identifier: u16) {
        assert!(identifier < self.count);
        // SAFETY: The initialized, page-aligned first entry contains the ABI's u16 tail. Rust and the kernel publish/observe this field atomically.
        let tail = unsafe {
            AtomicU16::from_ptr(types::BufRingEntry::tail(self.ring.as_ptr()).cast_mut())
        };
        let position = tail.load(Ordering::Relaxed);
        // SAFETY: A zeroed entry is valid and setters initialize every consumed field.
        let mut entry: types::BufRingEntry = unsafe { std::mem::zeroed() };
        entry.set_addr(self.pointer(identifier) as u64);
        entry.set_len(self.buffer_size as u32);
        entry.set_bid(identifier);
        // SAFETY: This unique buffer was either never published or returned by its sole lease. The masked slot is free. Copy only addr/len/bid: slot zero's final two bytes alias the shared atomic tail and must never be overwritten.
        unsafe {
            std::ptr::copy_nonoverlapping(
                (&entry as *const types::BufRingEntry).cast::<u8>(),
                self.ring
                    .as_ptr()
                    .add((position & (self.entries - 1)) as usize)
                    .cast(),
                14,
            );
        }
        tail.store(position.wrapping_add(1), Ordering::Release);
        self.available.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn pointer(&self, identifier: u16) -> *mut u8 {
        assert!(identifier < self.count);
        // SAFETY: Each identifier addresses its complete stable buffer inside this allocation.
        unsafe {
            self.buffers
                .as_ptr()
                .add(identifier as usize * self.buffer_size)
                .cast::<u8>()
                .cast_mut()
        }
    }
    /// Caller must own this buffer's completed lease and not republish it while borrowed.
    pub(super) unsafe fn bytes(&self, identifier: u16, offset: usize, length: usize) -> &[u8] {
        assert!(offset <= self.buffer_size && length <= self.buffer_size - offset);
        // SAFETY: The caller establishes initialized, exclusive lease access; bounds and identifier are validated.
        unsafe { std::slice::from_raw_parts(self.pointer(identifier).add(offset), length) }
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        // SAFETY: The original request deregistered before releasing its owner. Failure paths deliberately retain an extra owner instead. All leases and queued recycle commands have also retired.
        unsafe { dealloc(self.ring.as_ptr().cast(), self.layout) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_identifiers_are_bounded_and_reused_after_release() {
        let (selector, mut driver) = super::super::super::tests::pair(1);
        let controller = &selector.controllers[0];
        // Each live group represents a legal receiver, independently of whether its pool has been registered with the kernel yet.
        let mut groups = Vec::with_capacity(u16::MAX as usize + 1);
        for identifier in 0..=u16::MAX {
            let group = Group::new(controller).unwrap();
            assert_eq!(group.identifier, identifier);
            groups.push(group);
        }
        assert!(
            matches!(Group::new(controller), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
        );
        assert!(
            matches!(Pool::new(controller, RecvMsgMultishotOptions { buffers: 1, buffer_size: 8 }, 0), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
        );
        let released = groups.swap_remove(7);
        drop(released);
        let replacement = Group::new(controller).unwrap();
        assert_eq!(replacement.identifier, 7);
        drop(replacement);
        drop(groups);
        super::super::super::tests::drain(&selector, &mut driver);
    }
}
