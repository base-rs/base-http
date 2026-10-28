// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#[derive(Debug)]
pub(crate) struct SocketMessage<Buffers> {
    pub(crate) buffers: Buffers,
    pub(crate) address: Option<base_scheduler::SocketAddress>,
}
