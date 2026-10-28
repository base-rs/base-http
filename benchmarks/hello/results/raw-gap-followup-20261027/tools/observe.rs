//! Diagnostic counters only; instrumented throughput is never benchmark evidence.
use std::{future::Future, pin::Pin, sync::{Arc, OnceLock, atomic::{AtomicU64, Ordering}}, task::{Context, Poll, Wake, Waker}};

struct Metrics([AtomicU64; 8]);
static METRICS: OnceLock<&'static Metrics> = OnceLock::new();

fn metrics() -> &'static Metrics {
    METRICS.get_or_init(|| {
        use std::{fs::OpenOptions, os::fd::AsRawFd};
        let path = std::env::var("BASE_HTTP_POLL_COUNTS").expect("diagnostic counter path");
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path).unwrap();
        file.set_len(std::mem::size_of::<Metrics>() as u64).unwrap();
        unsafe extern "C" {
            fn mmap(address: *mut std::ffi::c_void, length: usize, prot: i32, flags: i32, fd: i32, offset: i64) -> *mut std::ffi::c_void;
        }
        // Linux-only diagnostic: fresh zero-filled shared mapping, retained until process exit.
        let memory = unsafe { mmap(std::ptr::null_mut(), std::mem::size_of::<Metrics>(), 3, 1, file.as_raw_fd(), 0) };
        assert_ne!(memory as isize, -1);
        unsafe { &*memory.cast::<Metrics>() }
    })
}

struct Meter { original: Waker, metrics: &'static Metrics, index: usize }
impl Wake for Meter {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) {
        self.metrics.0[self.index + 2].fetch_add(1, Ordering::Relaxed);
        self.original.wake_by_ref();
    }
}
pub struct Observed<F> { future: F, meter: Option<(Waker, Waker)>, index: usize }
pub fn supervisor<F: Future>(future: F) -> Observed<F> { Observed { future, meter: None, index: 0 } }
pub fn connection<F: Future>(future: F) -> Observed<F> { Observed { future, meter: None, index: 4 } }
impl<F: Future> Future for Observed<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The wrapper never moves its pinned future; only the separate meter is replaced.
        let this = unsafe { self.get_unchecked_mut() };
        let counts = metrics();
        counts.0[this.index].fetch_add(1, Ordering::Relaxed);
        if this.meter.as_ref().is_none_or(|(original, _)| !original.will_wake(cx.waker())) {
            let original = cx.waker().clone();
            let waker = Waker::from(Arc::new(Meter { original: original.clone(), metrics: counts, index: this.index }));
            this.meter = Some((original, waker));
        }
        let mut context = Context::from_waker(&this.meter.as_ref().unwrap().1);
        let result = unsafe { Pin::new_unchecked(&mut this.future) }.poll(&mut context);
        if result.is_pending() { counts.0[this.index + 1].fetch_add(1, Ordering::Relaxed); }
        result
    }
}
