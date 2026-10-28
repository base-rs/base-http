// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{SocketClose, SocketDomain};
use base_scheduler_selector::ReadinessSelector;
use futures_lite::future::block_on;

#[test]
fn public_output_preserves_order_and_progresses_without_its_waiter() {
    let scheduler = ReadinessSelector::new().unwrap();
    let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
        &scheduler,
        SocketDomain::Ipv4,
    ));
    block_on(async {
        base_scheduler_conformance::socket_stream_writing::ordering(&scheduler, &sender, &receiver)
            .await;
        base_scheduler_conformance::socket_stream_writing::independent_progress(
            &scheduler, &sender, &receiver,
        )
        .await;
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    });
}
