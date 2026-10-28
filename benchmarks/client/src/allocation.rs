//! Optional whole-process allocation diagnostics; disabled in throughput builds.

#[cfg(feature = "count-allocations")]
mod counting {
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        sync::atomic::{AtomicU64, Ordering::Relaxed},
    };
    static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
    static REALLOCATIONS: AtomicU64 = AtomicU64::new(0);
    static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);
    static BYTES: AtomicU64 = AtomicU64::new(0);
    static SIZES: [AtomicU64; 32769] = [const { AtomicU64::new(0) }; 32769];
    fn size(size: usize) {
        if let Some(count) = SIZES.get(size) {
            count.fetch_add(1, Relaxed);
        }
    }
    pub struct Allocator;
    // Forward each pointer/layout to System. Counting uses only atomics and
    // cannot recurse into the allocator.
    unsafe impl GlobalAlloc for Allocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Relaxed);
            BYTES.fetch_add(layout.size() as u64, Relaxed);
            size(layout.size());
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Relaxed);
            BYTES.fetch_add(layout.size() as u64, Relaxed);
            size(layout.size());
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            REALLOCATIONS.fetch_add(1, Relaxed);
            BYTES.fetch_add(size as u64, Relaxed);
            unsafe { System.realloc(ptr, layout, size) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            DEALLOCATIONS.fetch_add(1, Relaxed);
            unsafe { System.dealloc(ptr, layout) }
        }
    }
    pub fn counts() -> [u64; 4] {
        [
            ALLOCATIONS.load(Relaxed),
            REALLOCATIONS.load(Relaxed),
            DEALLOCATIONS.load(Relaxed),
            BYTES.load(Relaxed),
        ]
    }
    pub fn sizes() -> Vec<u64> {
        SIZES.iter().map(|count| count.load(Relaxed)).collect()
    }
}
#[cfg(feature = "count-allocations")]
#[global_allocator]
static ALLOCATOR: counting::Allocator = counting::Allocator;

pub struct Snapshot {
    #[cfg(feature = "count-allocations")]
    counts: [u64; 4],
    #[cfg(feature = "count-allocations")]
    sizes: Vec<u64>,
}
impl Snapshot {
    pub fn now() -> Self {
        #[cfg(feature = "count-allocations")]
        let sizes = counting::sizes();
        Self {
            #[cfg(feature = "count-allocations")]
            counts: counting::counts(),
            #[cfg(feature = "count-allocations")]
            sizes,
        }
    }
    pub fn finish(self) -> Self {
        #[cfg(feature = "count-allocations")]
        let end = counting::counts();
        Self {
            #[cfg(feature = "count-allocations")]
            counts: std::array::from_fn(|i| end[i] - self.counts[i]),
            #[cfg(feature = "count-allocations")]
            sizes: counting::sizes()
                .into_iter()
                .zip(self.sizes)
                .map(|(end, start)| end - start)
                .collect(),
        }
    }
    pub fn attach(self, result: &mut serde_json::Value) {
        #[cfg(feature = "count-allocations")]
        {
            let requests = result["requests"].as_u64().unwrap() as f64;
            let sizes: Vec<_> = self.sizes.iter().enumerate()
                .filter(|(_, count)| **count as f64 / requests > 0.05)
                .map(|(size, count)| serde_json::json!({"bytes": size, "allocations_per_response": *count as f64 / requests}))
                .collect();
            result["allocation_diagnostic"] = serde_json::json!({
                "scope": "whole_client_process_timed_phase",
                "allocations": self.counts[0], "reallocations": self.counts[1],
                "deallocations": self.counts[2], "requested_bytes": self.counts[3],
                "allocations_per_response": self.counts[0] as f64 / requests,
                "reallocations_per_response": self.counts[1] as f64 / requests,
                "requested_bytes_per_response": self.counts[3] as f64 / requests,
                "frequent_allocation_sizes_up_to_32768": sizes,
            });
        }
        #[cfg(not(feature = "count-allocations"))]
        let _ = result;
    }
}
