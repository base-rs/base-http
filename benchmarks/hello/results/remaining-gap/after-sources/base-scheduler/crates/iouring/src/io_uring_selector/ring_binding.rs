// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::Controller;
use std::sync::Arc;

// Ring-local values always travel with their owning endpoint. Polling-thread changes do not change this owner.
pub(super) struct RingBinding<T> {
    pub controller: Arc<Controller>,
    pub state: T,
}

impl<T> RingBinding<T> {
    pub fn new(controller: &Arc<Controller>, state: T) -> Self {
        Self {
            controller: Arc::clone(controller),
            state,
        }
    }

    pub fn matches(&self, controller: &Arc<Controller>) -> bool {
        Arc::ptr_eq(&self.controller, controller)
    }
}

// Cache only the current ring. Replaced registrations retire through their original owner.
pub(super) struct RingCache<T> {
    current: Option<RingBinding<T>>,
}

impl<T> Default for RingCache<T> {
    fn default() -> Self {
        Self { current: None }
    }
}

impl<T> RingCache<T> {
    pub fn matches(&self, controller: &Arc<Controller>) -> bool {
        self.current
            .as_ref()
            .is_some_and(|binding| binding.matches(controller))
    }

    pub fn take_current(&mut self) -> Option<T> {
        self.current.take().map(|binding| binding.state)
    }

    pub fn get_or_insert(
        &mut self,
        controller: &Arc<Controller>,
        create: impl FnOnce() -> T,
    ) -> &T {
        if !self.matches(controller) {
            self.current = Some(RingBinding::new(controller, create()));
        }
        &self.current.as_ref().unwrap().state
    }

    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.current.iter().map(|binding| &binding.state)
    }
}
