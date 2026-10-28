// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::{block_on, poll_once};
use std::sync::Barrier;

const AT_LIMIT: &str = "pool counter reached its limit and reused a released owner";

// Aborting skips the normal coverage runtime shutdown. Save the child's counters before delegating to the real process abort; a failed flush must not change termination behavior.
pub(super) fn abort() -> ! {
    #[cfg(coverage)]
    unsafe {
        unsafe extern "C" {
            fn __llvm_profile_write_file() -> std::ffi::c_int;
        }
        // SAFETY: Coverage-instrumented test binaries link this LLVM profiling-runtime entry point, which takes no arguments and retains no Rust references.
        let _ = __llvm_profile_write_file();
    }
    std::process::abort();
}

fn assert_aborts(name: &str, operation: impl FnOnce()) {
    const CHILD: &str = "CORE_SCHEDULER_POOL_OVERFLOW_TEST";
    if std::env::var(CHILD).as_deref() == Ok(name) {
        #[cfg(unix)]
        {
            let limit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            // SAFETY: This valid limit only disables core dumps in the isolated child process.
            assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_CORE, &limit) }, 0);
        }
        operation();
        panic!("counter overflow did not abort");
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env(CHILD, name)
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(AT_LIMIT),
        "the child aborted before reaching the checked boundary: {output:?}"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(output.status.signal(), Some(libc::SIGABRT), "{output:?}");
    }
    #[cfg(windows)]
    assert_eq!(
        output.status.code().map(|code| code as u32),
        Some(0xC0000409), // Windows fast-fail termination from std::process::abort.
        "{output:?}"
    );
}

#[test]
#[cfg_attr(miri, ignore = "requires subprocess termination")]
fn frozen_reader_limit_aborts_before_the_count_can_wrap() {
    assert_aborts(
        "heap_buffer_pool::tests::frozen_reader_limit_aborts_before_the_count_can_wrap",
        || {
            let pool = HeapBufferPool::new(1, 8).unwrap();
            let frozen = pool.try_acquire().unwrap().freeze();
            let mut readers: Vec<_> = (1..COUNT_LIMIT).map(|_| frozen.clone()).collect();
            assert!(pool.try_acquire().is_none());
            drop(readers.pop().unwrap());
            readers.push(frozen.clone());
            assert_eq!(readers.len() + 1, COUNT_LIMIT);
            eprintln!("{AT_LIMIT}");
            drop(frozen.clone());
        },
    );
}

#[test]
#[cfg_attr(miri, ignore = "requires subprocess termination")]
fn waiter_limit_aborts_before_the_count_can_wrap() {
    assert_aborts(
        "heap_buffer_pool::tests::waiter_limit_aborts_before_the_count_can_wrap",
        || {
            let pool = HeapBufferPool::new(1, 8).unwrap();
            let _lease = pool.try_acquire().unwrap();
            let never = Cancellation::never();
            let mut waiters = Vec::new();
            for _ in 0..COUNT_LIMIT {
                let mut waiter = Box::pin(pool.acquire(&never));
                assert!(block_on(poll_once(waiter.as_mut())).is_none());
                waiters.push(waiter);
            }
            drop(waiters.pop().unwrap());
            let mut replacement = Box::pin(pool.acquire(&never));
            assert!(block_on(poll_once(replacement.as_mut())).is_none());
            waiters.push(replacement);
            assert_eq!(waiters.len(), COUNT_LIMIT);
            eprintln!("{AT_LIMIT}");
            let mut excess = pin!(pool.acquire(&never));
            let _ = block_on(poll_once(excess.as_mut()));
        },
    );
}

#[test]
fn closing_retires_idle_tokens_across_availability_words() {
    for capacity in [1, WIDTH, WIDTH + 1, WIDTH * 2 + 3] {
        let pool = HeapBufferPool::new(capacity, 8).unwrap();
        let slab = Arc::downgrade(&pool.0.slab);
        assert_eq!(slab.strong_count(), capacity + 1);
        drop(pool);
        assert!(slab.upgrade().is_none());
    }
}

#[test]
fn slot_tokens_survive_pool_close_and_all_frozen_views() {
    let pool = HeapBufferPool::new(3, 8).unwrap();
    let slab = Arc::downgrade(&pool.0.slab);
    let first = pool.try_acquire().unwrap();
    let frozen = pool.try_acquire().unwrap().freeze();
    let view = frozen.view(2..6);
    assert_eq!(slab.strong_count(), 4);
    drop(pool);
    assert_eq!(slab.strong_count(), 2);
    assert_eq!(first.as_ref(), [0; 8]);
    assert_eq!(view.as_ref(), [0; 4]);
    drop(first);
    drop(frozen);
    assert_eq!(slab.strong_count(), 1);
    std::thread::spawn(move || drop(view)).join().unwrap();
    assert!(slab.upgrade().is_none());
}

#[test]
fn concurrent_returns_and_pool_close_reclaim_every_token() {
    for _ in 0..if cfg!(miri) { 4 } else { 300 } {
        let pool = HeapBufferPool::new(8, 8).unwrap();
        let slab = Arc::downgrade(&pool.0.slab);
        let mutable = pool.try_acquire().unwrap();
        let frozen = pool.try_acquire().unwrap().freeze();
        let view = frozen.view(..4);
        let barrier = Arc::new(Barrier::new(4));
        std::thread::scope(|scope| {
            for owner in [
                Box::new(move || drop(mutable)) as Box<dyn FnOnce() + Send>,
                Box::new(move || drop(frozen)),
                Box::new(move || drop(view)),
            ] {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    owner();
                });
            }
            barrier.wait();
            drop(pool);
        });
        assert!(slab.upgrade().is_none());
    }
}

#[test]
fn abandoned_exhaustion_restores_the_fast_return_path() {
    let pool = HeapBufferPool::new(WIDTH + 1, 8).unwrap();
    let buffers: Vec<_> = (0..pool.capacity())
        .map(|_| pool.try_acquire().unwrap())
        .collect();
    let token = Cancellation::never();
    {
        let mut waiting = pin!(pool.acquire(&token));
        assert!(block_on(poll_once(waiting.as_mut())).is_none());
        assert!(
            pool.0
                .slab
                .free
                .iter()
                .all(|word| word.load(Ordering::Relaxed) & WAITING != 0)
        );
    }
    assert!(
        pool.0
            .slab
            .free
            .iter()
            .all(|word| word.load(Ordering::Relaxed) & WAITING == 0)
    );
    let slab = Arc::downgrade(&pool.0.slab);
    drop(buffers);
    assert_eq!(slab.strong_count(), pool.capacity() + 1);
    drop(pool);
    assert!(slab.upgrade().is_none());
}

#[test]
fn notification_can_synchronously_acquire_and_retain_a_returned_slot() {
    use std::task::{Context, Wake, Waker};

    struct AcquiringWaiter {
        pool: HeapBufferPool,
        acquired: Mutex<Option<BufferLease>>,
    }
    impl Wake for AcquiringWaiter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            *self.acquired.lock().unwrap() = self.pool.try_acquire();
        }
    }

    let pool = HeapBufferPool::new(1, 8).unwrap();
    let lease = pool.try_acquire().unwrap();
    let callback = Arc::new(AcquiringWaiter {
        pool: pool.clone(),
        acquired: Mutex::new(None),
    });
    let waker = Waker::from(callback.clone());
    let never = Cancellation::never();
    let mut waiting = Box::pin(pool.acquire(&never));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(lease);
    assert!(pool.try_acquire().is_none());
    let returned = callback.acquired.lock().unwrap().take().unwrap();
    assert_eq!(returned.as_ref(), [0; 8]);
    drop(waiting);
    drop(returned);
    assert!(pool.try_acquire().is_some());
}
