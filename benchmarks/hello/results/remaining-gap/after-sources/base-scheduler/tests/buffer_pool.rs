// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{
    BufferPool, BufferPoolOptions, Cancellation, HeapBufferPool, IoBuf, ReceiveBuffer,
};
#[path = "support/buffer_pool.rs"]
mod pool_support;
use futures_lite::future::{block_on, poll_once};
use std::pin::pin;

#[test]
fn pools_validate_capacity_and_recycle_stable_initialized_storage() {
    for (count, size) in [(0, 1), (1, 0), (usize::MAX, 2)] {
        assert!(HeapBufferPool::new(count, size).is_err());
    }
    let pool = HeapBufferPool::new(1, 16).unwrap();
    assert_eq!(pool.capacity(), 1);
    assert_eq!(pool.buffer_size(), 16);
    let mut buffer = pool.try_acquire().unwrap();
    let pointer = buffer.as_ref().as_ptr();
    buffer.as_mut().fill(42);
    assert!(pool.try_acquire().is_none());
    drop(buffer);
    let buffer = pool.try_acquire().unwrap();
    assert_eq!(pointer, buffer.as_ref().as_ptr());
    assert_eq!(buffer.as_ref(), [42; 16]);
}

#[test]
fn frozen_views_hold_storage_until_the_final_cross_thread_return() {
    let pool = HeapBufferPool::new(1, 16).unwrap();
    let mut buffer = pool.try_acquire().unwrap();
    buffer.as_mut()[..6].copy_from_slice(b"abcdef");
    buffer.truncate(6);
    let pointer = buffer.as_ref().as_ptr() as usize;
    let frozen = buffer.freeze();
    let first = frozen.view(..3);
    let last = frozen.view(3..);
    assert!(format!("{frozen:?}").contains('6'));
    drop(frozen);
    drop(first);
    assert!(pool.try_acquire().is_none());
    std::thread::spawn(move || {
        assert_eq!(last.as_ref(), b"def");
        drop(last);
    })
    .join()
    .unwrap();
    assert_eq!(
        pool.try_acquire().unwrap().as_ref().as_ptr() as usize,
        pointer
    );
}

#[test]
fn leases_outlive_the_pool_handle_and_can_be_used_as_owned_ranges() {
    let pool = HeapBufferPool::new(1, 16).unwrap();
    let buffer = pool.try_acquire().unwrap();
    drop(pool);
    let range = buffer.slice(4..8);
    assert_eq!(range.as_ref(), [0; 4]);
    let buffer = range.into_inner();
    assert!(format!("{buffer:?}").contains("16"));
    let frozen = buffer.freeze();
    assert_eq!(frozen.as_ref(), [0; 16]);
}

#[test]
fn exhaustion_waits_for_capacity_and_cancellation_does_not_take_a_buffer() {
    block_on(async {
        let pool = HeapBufferPool::new(1, 16).unwrap();
        let lease = pool.try_acquire().unwrap();
        let token = Cancellation::new();
        let mut waiting = pin!(pool.acquire(&token));
        assert!(poll_once(waiting.as_mut()).await.is_none());
        token.cancel();
        assert!(waiting.await.is_err());
        assert!(pool.try_acquire().is_none());
        drop(lease);
        assert!(pool.acquire(&token).await.is_err());
        let never = Cancellation::never();
        let lease = pool.acquire(&never).await.unwrap();
        let other = pool.clone();
        let mut waiting = pin!(other.acquire(&never));
        assert!(poll_once(waiting.as_mut()).await.is_none());
        std::thread::spawn(move || drop(lease)).join().unwrap();
        assert_eq!(waiting.await.unwrap().as_ref().len(), 16);
    });
}

#[test]
fn abandoning_waits_and_forgetting_owners_are_safe() {
    let pool = HeapBufferPool::new(1, 16).unwrap();
    let lease = pool.try_acquire().unwrap();
    let token = Cancellation::never();
    {
        let mut wait = pin!(pool.acquire(&token));
        assert!(block_on(poll_once(wait.as_mut())).is_none());
    }
    drop(lease);
    let lease = pool.try_acquire().unwrap();
    std::mem::forget(lease);
    assert!(pool.try_acquire().is_none());
}

#[cfg(not(miri))]
#[test]
fn steady_state_acquire_freeze_views_and_return_allocate_nothing() {
    let pool = HeapBufferPool::new(2, 64).unwrap();
    let info = allocation_counter::measure(|| {
        for _ in 0..128 {
            let mut lease = pool.try_acquire().unwrap();
            lease.as_mut().fill(42);
            lease.truncate(32);
            let frozen = lease.freeze();
            let first = frozen.view(..16);
            let second = frozen.view(16..);
            std::hint::black_box((first.as_ref(), second.as_ref()));
        }
    });
    assert_eq!(info.count_total, 0);
}

#[test]
#[should_panic(expected = "buffer length out of bounds")]
fn leases_cannot_expose_bytes_outside_the_initialized_prefix() {
    let mut lease = HeapBufferPool::new(1, 16).unwrap().try_acquire().unwrap();
    lease.truncate(17);
}

#[test]
fn many_readers_return_each_slot_once() {
    let pool = HeapBufferPool::new(4, 64).unwrap();
    for _ in 0..20 {
        let buffers: Vec<_> = (0..4)
            .map(|_| pool.try_acquire().unwrap().freeze())
            .collect();
        std::thread::scope(|scope| {
            for buffer in &buffers {
                for _ in 0..4 {
                    let view = buffer.clone();
                    scope.spawn(move || assert_eq!(view.as_ref(), [0; 64]));
                }
            }
        });
        assert!(pool.try_acquire().is_none());
        drop(buffers);
    }
    let leases: Vec<_> = (0..4).map(|_| pool.try_acquire().unwrap()).collect();
    assert!(pool.try_acquire().is_none());
    drop(leases);
}

#[test]
fn availability_words_preserve_exact_capacity_and_stable_storage() {
    for count in [usize::BITS as usize, usize::BITS as usize + 3] {
        let pool = HeapBufferPool::new(count, 8).unwrap();
        let buffers: Vec<_> = (0..count).map(|_| pool.try_acquire().unwrap()).collect();
        let pointers: std::collections::HashSet<_> = buffers
            .iter()
            .map(|buffer| buffer.as_ref().as_ptr() as usize)
            .collect();
        assert_eq!(pointers.len(), count);
        assert!(pool.try_acquire().is_none());
        std::thread::spawn(move || drop(buffers)).join().unwrap();
        let buffers: Vec<_> = (0..count).map(|_| pool.try_acquire().unwrap()).collect();
        assert!(
            buffers
                .iter()
                .all(|buffer| pointers.contains(&(buffer.as_ref().as_ptr() as usize)))
        );
        assert!(pool.try_acquire().is_none());
    }
}

#[test]
fn concurrent_checkouts_exclusively_own_their_slots() {
    let pool = HeapBufferPool::new(8, 64).unwrap();
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        for value in 0..8 {
            let pool = &pool;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..if cfg!(miri) { 10 } else { 1000 } {
                    let mut buffer = pool.try_acquire().unwrap();
                    buffer.as_mut().fill(value);
                    std::thread::yield_now();
                    assert_eq!(buffer.as_ref(), [value; 64]);
                    let frozen = buffer.freeze();
                    assert_eq!(frozen.view(..8).as_ref(), [value; 8]);
                }
            });
        }
    });
    let buffers: Vec<_> = (0..8).map(|_| pool.try_acquire().unwrap()).collect();
    assert!(pool.try_acquire().is_none());
    drop(buffers);
}

#[test]
fn each_return_wakes_a_distinct_waiter_before_either_is_polled() {
    block_on(async {
        let pool = HeapBufferPool::new(2, 16).unwrap();
        let first = pool.try_acquire().unwrap();
        let second = pool.try_acquire().unwrap();
        let never = Cancellation::never();
        let mut first_waiter = pin!(pool.acquire(&never));
        let mut second_waiter = pin!(pool.acquire(&never));
        assert!(poll_once(first_waiter.as_mut()).await.is_none());
        assert!(poll_once(second_waiter.as_mut()).await.is_none());
        drop((first, second));
        let first = poll_once(first_waiter.as_mut()).await.unwrap().unwrap();
        // Keep this lease: a second free slot must wake the second waiter independently.
        let second = poll_once(second_waiter.as_mut()).await.unwrap().unwrap();
        assert!(pool.try_acquire().is_none());
        drop((first, second));
    });
}

#[test]
fn pool_contract_preserves_alignment_prefixes_and_shared_capacity() {
    async fn check<P: BufferPool>(pool: P, alignment: usize) {
        assert_eq!(pool.capacity(), 2);
        assert_eq!(pool.buffer_size(), 257);
        assert_eq!(pool.alignment(), alignment);
        let mut first = pool.try_acquire().unwrap();
        assert_eq!(first.as_ref().as_ptr() as usize % alignment, 0);
        first.as_mut()[..6].copy_from_slice(b"abcdef");
        ReceiveBuffer::truncate(&mut first, 6);
        let frozen = first.freeze();
        let view = frozen.clone().slice(1..5);
        drop(frozen);
        let other = pool.clone();
        let second = other.acquire(&Cancellation::never()).await.unwrap();
        assert_eq!(second.as_ref().as_ptr() as usize % alignment, 0);
        assert!(pool.try_acquire().is_none());
        drop(second);
        let token = Cancellation::new();
        token.cancel();
        assert!(other.acquire(&token).await.is_err());
        let second = other.try_acquire().unwrap();
        let never = Cancellation::never();
        let mut waiting = pin!(pool.acquire(&never));
        assert!(poll_once(waiting.as_mut()).await.is_none());
        std::thread::spawn(move || {
            assert_eq!(view.as_ref(), b"bcde");
            drop(view);
        })
        .join()
        .unwrap();
        let first = waiting.await.unwrap();
        assert_eq!(first.as_ref().len(), 257);
        assert_eq!(&first.as_ref()[..6], b"abcdef");
        drop((first, second));
    }
    for alignment in [1, 8, 4096, 65536] {
        let pool = HeapBufferPool::with_options(BufferPoolOptions {
            alignment,
            ..BufferPoolOptions::new(2, 257)
        })
        .unwrap();
        block_on(check(pool.clone(), alignment));
        block_on(check(pool_support::TestPool(pool), alignment));
    }
}

#[test]
fn invalid_alignment_and_unrepresentable_padded_capacity_are_rejected() {
    for (buffers, buffer_size, alignment) in [
        (1, 8, 0),
        (1, 8, 3),
        (0, 8, 4096),
        (1, 0, 4096),
        (1, usize::MAX, 4096),
        (usize::MAX, 1, 1),
        (2, isize::MAX as usize, 1),
        (usize::MAX, 8, 4096),
    ] {
        assert!(matches!(HeapBufferPool::with_options(BufferPoolOptions {
            buffers, buffer_size, alignment,
        }), Err(error) if error.kind() == std::io::ErrorKind::InvalidInput));
    }
}

#[cfg(not(miri))]
#[test]
fn generic_pool_operations_allocate_nothing_after_setup() {
    fn exercise<P: BufferPool>(pool: &P) {
        for _ in 0..128 {
            let mut buffer = pool.try_acquire().unwrap();
            buffer.as_mut().fill(42);
            buffer.truncate(16);
            let frozen = buffer.freeze();
            std::hint::black_box(frozen.clone().slice(2..8));
        }
    }
    let pool = HeapBufferPool::with_options(BufferPoolOptions {
        alignment: 4096,
        ..BufferPoolOptions::new(2, 64)
    })
    .unwrap();
    let alternate = pool_support::TestPool(pool.clone());
    let allocations = allocation_counter::measure(|| {
        exercise(&pool);
        exercise(&alternate);
    });
    assert_eq!(allocations.count_total, 0);
}

#[test]
fn aligned_slots_share_one_slab_without_aliasing_other_live_slots() {
    for alignment in [1, 8, 4096] {
        let pool = HeapBufferPool::with_options(BufferPoolOptions {
            alignment,
            ..BufferPoolOptions::new(3, 257)
        })
        .unwrap();
        let mut first = pool.try_acquire().unwrap();
        let mut second = pool.try_acquire().unwrap();
        let mut third = pool.try_acquire().unwrap();
        let stride = (257 + alignment - 1) & !(alignment - 1);
        assert_eq!(
            second.as_ref().as_ptr() as usize - first.as_ref().as_ptr() as usize,
            stride
        );
        assert_eq!(
            third.as_ref().as_ptr() as usize - second.as_ref().as_ptr() as usize,
            stride
        );
        first.as_mut().fill(1);
        second.as_mut().fill(2);
        third.as_mut().fill(3);
        let retained = first.freeze();
        let borrowed = retained.as_ref();
        let writable = second.as_mut();
        writable.fill(4);
        assert_eq!(borrowed, [1; 257]);
        assert_eq!(third.as_ref(), [3; 257]);
        drop(pool);
        assert_eq!(writable, [4; 257]);
        assert_eq!(retained.view(0..1).as_ref(), [1]);
    }
}

#[test]
fn portable_pool_factories_need_only_the_ownership_contract() {
    use base_scheduler::BufferPooling;
    struct Portable;
    impl BufferPooling for Portable {
        type Pool = HeapBufferPool;
        fn buffer_pool(&self, options: BufferPoolOptions) -> std::io::Result<Self::Pool> {
            HeapBufferPool::with_options(options)
        }
    }
    let portable = Portable;
    let options = BufferPoolOptions::new(2, 8);
    assert_eq!(portable.buffer_pool(options).unwrap().capacity(), 2);
}

#[test]
fn leases_and_views_keep_their_send_and_sync_contracts() {
    fn send_and_sync<T: Send + Sync>() {}
    fn send<T: Send>(_: T) {}
    send_and_sync::<HeapBufferPool>();
    send_and_sync::<base_scheduler::BufferLease>();
    send_and_sync::<base_scheduler::FrozenBuffer>();
    let pool = HeapBufferPool::new(1, 8).unwrap();
    send(pool.acquire(&Cancellation::never()));
}

#[test]
fn forgetting_a_frozen_view_never_recycles_its_slot() {
    let pool = HeapBufferPool::new(2, 8).unwrap();
    let mut lease = pool.try_acquire().unwrap();
    lease.as_mut().fill(42);
    let frozen = lease.freeze();
    std::mem::forget(frozen.view(..4));
    let borrowed = frozen.as_ref();
    for value in 0..8 {
        let mut other = pool.try_acquire().unwrap();
        assert!(pool.try_acquire().is_none());
        other.as_mut().fill(value);
        assert_eq!(borrowed, [42; 8]);
    }
    drop(pool);
    assert_eq!(borrowed, [42; 8]);
}
