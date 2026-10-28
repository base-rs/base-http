//! Isolate pool admission/return, without HTTP, sockets or exchange adapters.
use super::*;
use base_pool::{Acquisition, Admission, MultiplexPool, MultiplexUse};
use futures_util::task::{ArcWake, waker_ref};
use std::{
    pin::pin,
    sync::atomic::AtomicUsize,
    task::{Context, Poll},
};

struct Wakes(AtomicUsize);
impl ArcWake for Wakes {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::Relaxed);
    }
}

async fn cycle(
    pool: &MultiplexPool<usize>,
    slot: &mut Option<MultiplexUse<usize>>,
    cancellation: &Cancellation,
) -> Result<()> {
    slot.take().unwrap().complete();
    let Acquisition::Existing(reservation) = pool.acquire_or_reserve(cancellation).await? else {
        return Err("pool unexpectedly requested construction".into());
    };
    *slot = Some(reservation.activate()?);
    Ok(())
}

pub(super) async fn run(args: &Args) -> Result<serde_json::Value> {
    let observed = match args.implementation.as_str() {
        "pool" => false,
        "pool-observed" => true,
        _ => return Err("pool role expects pool or pool-observed".into()),
    };
    let pool = MultiplexPool::new(base_pool::Options {
        max_resources: args.concurrency,
        max_constructing: args.concurrency,
        max_waiters: args.concurrency,
    })?;
    let cancel = Cancellation::never();
    let mut active = Vec::with_capacity(args.concurrency);
    for index in 0..args.concurrency {
        let Acquisition::Build(build) = pool.acquire_or_reserve(&cancel).await? else {
            return Err("pool unexpectedly reused an occupied resource".into());
        };
        build.commit(index, Admission::concurrent(1))?;
        let Acquisition::Existing(reservation) = pool.acquire_or_reserve(&cancel).await? else {
            return Err("pool did not select its new resource".into());
        };
        active.push(Some(reservation.activate()?));
    }
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = waker_ref(&wakes);
    let mut cx = Context::from_waker(&waker);
    let retirement = pool.next_retirement(&cancel);
    let mut retirement = pin!(retirement);
    if observed {
        assert!(retirement.as_mut().poll(&mut cx).is_pending());
    }
    let warmup = Instant::now() + Duration::from_secs_f64(args.warmup);
    let mut index = 0;
    while Instant::now() < warmup {
        cycle(&pool, &mut active[index], &cancel).await?;
        if observed {
            assert!(retirement.as_mut().poll(&mut cx).is_pending());
        }
        index = (index + 1) % args.concurrency;
    }
    wakes.0.store(0, Ordering::Relaxed);
    let start = Instant::now();
    let cpu_start = cpu_seconds();
    let allocations = allocation::Snapshot::now();
    let deadline = start + Duration::from_secs_f64(args.seconds);
    let mut operations = 0_u64;
    while Instant::now() < deadline {
        cycle(&pool, &mut active[index], &cancel).await?;
        if observed {
            assert!(matches!(retirement.as_mut().poll(&mut cx), Poll::Pending));
        }
        operations += 1;
        index = (index + 1) % args.concurrency;
    }
    let cpu = cpu_seconds() - cpu_start;
    let allocations = allocations.finish();
    let elapsed = start.elapsed().as_secs_f64();
    let mut result = json!({
        "implementation": args.implementation, "resources": args.concurrency,
        "requests": operations, "operations": operations,
        "elapsed_seconds": elapsed, "operations_per_second": operations as f64 / elapsed,
        "cpu_us_per_operation": cpu * 1e6 / operations as f64,
        "retirement_wakes": wakes.0.load(Ordering::Relaxed),
        "retirement_wakes_per_operation": wakes.0.load(Ordering::Relaxed) as f64 / operations as f64,
        "note": "Synchronous capacity-one pool cycles, optional retirement poller; no HTTP or I/O."
    });
    allocations.attach(&mut result);
    // The retirement future is only a borrowed waiter. Closing below makes
    // it ready; release actual resource retirement obligations explicitly.
    for slot in &mut active {
        slot.take().unwrap().complete();
    }
    assert_eq!(pool.stats().uses, 0);
    assert_eq!(pool.stats().live, args.concurrency);
    pool.begin_close();
    while let Some(retired) = pool.try_retirement() {
        retired.complete();
    }
    pool.wait_closed(&cancel).await?;
    assert_eq!(pool.stats().total(), 0);
    Ok(result)
}
