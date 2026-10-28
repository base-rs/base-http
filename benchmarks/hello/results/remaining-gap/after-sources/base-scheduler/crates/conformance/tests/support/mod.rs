// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::mpsc,
    time::Duration,
};

pub fn run(test: impl FnOnce() + Send + 'static) {
    let (done, completion) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(catch_unwind(AssertUnwindSafe(test)));
    });
    if let Err(payload) = completion
        .recv_timeout(Duration::from_secs(10))
        .expect("conformance test deadline exceeded")
    {
        std::panic::resume_unwind(payload);
    }
}
