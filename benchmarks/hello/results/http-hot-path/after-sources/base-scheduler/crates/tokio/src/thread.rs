// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{io, thread};

#[cfg(test)]
thread_local! {
    pub(crate) static FAIL_NEXT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) fn start(name: &str, run: impl FnOnce() + Send + 'static) -> io::Result<()> {
    #[cfg(test)]
    if FAIL_NEXT.replace(false) {
        return Err(io::Error::other("injected thread startup failure"));
    }
    thread::Builder::new()
        .name(name.into())
        .spawn(run)
        .map(drop)
}
