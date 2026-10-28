// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
#[test]
fn host_panic_payload_is_preserved_by_the_join_boundary() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    // Exercise the host-error boundary independently of the adapter's user-future panic guard.
    let inner = runtime.spawn(async { std::panic::panic_any(String::from("host panic")) });
    let task: TokioTask<()> = TokioTask {
        inner,
        status: Default::default(),
    };
    let Err(TaskError::Panicked(payload)) = runtime.block_on(task) else {
        panic!("lost host panic")
    };
    assert_eq!(*payload.downcast::<String>().unwrap(), "host panic");
}
