// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::sync::{Mutex, atomic::AtomicU32};

const UNREGISTERED: u32 = u32::MAX;
const FALLBACK: u32 = u32::MAX - 1;
const RETIRED: u32 = u32::MAX - 2;

// Only the owning worker accesses this table, under its controller's cold-path lock. Slots are recycled after the kernel removes their registered file.
pub(super) struct Files {
    capacity: u32,
    initialized: bool,
    disabled: bool,
    next: u32,
    free: Vec<u32>,
}

impl Files {
    pub fn new(capacity: u32) -> Self {
        Self {
            capacity: capacity.min(RETIRED),
            initialized: false,
            disabled: capacity == 0,
            next: 0,
            free: Vec::new(),
        }
    }

    fn insert(&mut self, ring: &io_uring::Submitter<'_>, file: &StdFile) -> Option<u32> {
        if self.disabled {
            return None;
        }
        if !self.initialized {
            if syscall("fixed-file-table", || {
                ring.register_files_sparse(self.capacity)
            })
            .is_err()
            {
                self.disabled = true;
                return None;
            }
            self.initialized = true;
        }
        let index = self.free.pop().or_else(|| {
            if self.next == self.capacity {
                None
            } else {
                let index = self.next;
                self.next += 1;
                Some(index)
            }
        })?;
        if syscall("fixed-file-register", || {
            ring.register_files_update(index, &[file.as_raw_fd()])
        })
        .is_err()
        {
            self.free.push(index);
            return None;
        }
        Some(index)
    }

    pub fn remove(&mut self, ring: &io_uring::Submitter<'_>, index: u32) -> io::Result<()> {
        syscall("fixed-file-release", || {
            ring.register_files_update(index, &[-1])
        })?;
        self.free.push(index);
        Ok(())
    }
}

// The monitor and its current request share this lease. The file remains retained through queued release; no request can use a slot after release begins.
pub(super) struct RegisteredFile {
    pub file: Arc<StdFile>,
    controller: Arc<Controller>,
    index: AtomicU32,
    retirement: Arc<Retirement>,
}

impl RegisteredFile {
    pub fn new(controller: &Arc<Controller>, file: &Arc<StdFile>) -> Arc<Self> {
        Arc::new(Self {
            file: Arc::clone(file),
            controller: Arc::clone(controller),
            index: AtomicU32::new(UNREGISTERED),
            retirement: Arc::new(Retirement {
                result: Mutex::new(None),
                ready: AtomicBool::new(false),
                finished: CompletionEvent::new(),
            }),
        })
    }

    pub fn prepare(&self, ring: &io_uring::Submitter<'_>) {
        if self.index.load(Ordering::Relaxed) == UNREGISTERED {
            let index = self
                .controller
                .files
                .lock()
                .unwrap()
                .insert(ring, &self.file)
                .unwrap_or(FALLBACK);
            self.index.store(index, Ordering::Relaxed);
        }
    }

    pub fn index(&self) -> Option<types::Fixed> {
        let index = self.index.load(Ordering::Relaxed);
        (index < RETIRED).then_some(types::Fixed(index))
    }

    pub fn retire(&self) {
        let index = self.index.swap(RETIRED, Ordering::Relaxed);
        if index < RETIRED {
            let _ = self.controller.send(Command::FileRelease(Release {
                index,
                file: Some(Arc::clone(&self.file)),
                retirement: Some(Arc::clone(&self.retirement)),
            }));
        } else if index != RETIRED {
            self.retirement.publish(Ok(()));
        }
    }

    pub fn released(&self) -> bool {
        self.retirement.ready.load(Ordering::Acquire)
            && self
                .retirement
                .result
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(Result::is_ok)
    }

    pub async fn closed(&self) -> io::Result<()> {
        loop {
            event_listener::listener!(self.retirement.finished => listener);
            if let Some(result) = self.retirement.result.lock().unwrap().as_ref() {
                return result.as_ref().copied().map_err(|error| {
                    error
                        .raw_os_error()
                        .map(io::Error::from_raw_os_error)
                        .unwrap_or_else(|| io::Error::new(error.kind(), error.to_string()))
                });
            }
            listener.await;
        }
    }
}

impl Drop for RegisteredFile {
    fn drop(&mut self) {
        self.retire();
    }
}

struct Retirement {
    result: Mutex<Option<io::Result<()>>>,
    ready: AtomicBool,
    finished: CompletionEvent,
}
impl Retirement {
    fn publish(&self, result: io::Result<()>) {
        *self.result.lock().unwrap() = Some(result);
        self.ready.store(true, Ordering::Release);
        self.finished.notify(usize::MAX);
    }
}

pub(crate) struct Release {
    index: u32,
    file: Option<Arc<StdFile>>,
    retirement: Option<Arc<Retirement>>,
}
impl Release {
    pub(super) fn finish(mut self, ring: &io_uring::Submitter<'_>, controller: &Controller) {
        let result = controller.files.lock().unwrap().remove(ring, self.index);
        drop(self.file.take());
        self.retirement.take().unwrap().publish(result);
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        drop(self.file.take());
        if let Some(retirement) = self.retirement.take() {
            retirement.publish(Err(io::ErrorKind::BrokenPipe.into()));
        }
    }
}
