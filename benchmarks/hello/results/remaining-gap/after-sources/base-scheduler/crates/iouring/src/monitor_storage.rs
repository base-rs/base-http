// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::SocketMonitor;
use std::{ops::Deref, sync::Arc};

// The resource handle requests retirement when dropped. A ring submission independently retains the same stable allocation until its final CQE.
pub(crate) struct MonitorStorage {
    state: Arc<SocketMonitor>,
    application: bool,
}
impl MonitorStorage {
    pub fn new(state: SocketMonitor) -> Self {
        Self {
            state: Arc::new(state),
            application: true,
        }
    }
    pub fn retained(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            application: false,
        }
    }
    pub fn as_ptr(&self) -> *const SocketMonitor {
        Arc::as_ptr(&self.state)
    }
    #[cfg(test)]
    pub fn downgrade(&self) -> std::sync::Weak<SocketMonitor> {
        Arc::downgrade(&self.state)
    }
}
impl Deref for MonitorStorage {
    type Target = SocketMonitor;
    fn deref(&self) -> &SocketMonitor {
        &self.state
    }
}
impl AsRef<SocketMonitor> for MonitorStorage {
    fn as_ref(&self) -> &SocketMonitor {
        &self.state
    }
}
impl Drop for MonitorStorage {
    fn drop(&mut self) {
        if self.application {
            self.registration().retire(false);
        }
    }
}
