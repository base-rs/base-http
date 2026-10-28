// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    IoBuf, IoBufMut, SocketAddressView, SocketRecvMessage, SocketRecvMessageMut, SocketRecvResult,
};

/// A received payload owner paired with portable receive metadata. Other message implementations may retain their backend's native metadata instead.
pub struct OwnedSocketRecvMessage<Buffer: IoBuf> {
    result: SocketRecvResult,
    payload: Buffer,
}

impl<B: IoBuf> OwnedSocketRecvMessage<B> {
    /// `payload` must expose exactly the received bytes reported by `result.bytes`.
    pub fn new(result: SocketRecvResult, payload: B) -> Self {
        assert_eq!(
            result.bytes,
            payload.as_ref().len(),
            "receive metadata and payload lengths differ"
        );
        Self { result, payload }
    }
}

impl<B: IoBuf> SocketRecvMessage for OwnedSocketRecvMessage<B> {
    type Payload = B;

    fn address(&self) -> Option<impl SocketAddressView + '_> {
        self.result.address.as_ref()
    }

    fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    fn truncated(&self) -> bool {
        self.result.truncated
    }

    fn control_truncated(&self) -> bool {
        self.result.control_truncated
    }

    fn into_payload(self) -> B {
        self.payload
    }
}

impl<B: IoBufMut> SocketRecvMessageMut for OwnedSocketRecvMessage<B> {
    fn payload_mut(&mut self) -> &mut [u8] {
        self.payload.as_mut()
    }
}
