// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::AddressInfoView;

/// An owned resolver result whose representation is chosen by the backend. Borrowed entry views remain valid only while the result is borrowed. Iteration must preserve candidate ordering, duplicates, and native socket parameters without requiring a copied collection or per-entry reference counts.
///
/// Native implementations can traverse an owned `addrinfo` list directly and free it once through the matching native deallocator on drop. Validation may inspect that list without materializing portable records. Concrete result types may expose additional native accessors. Arrays, vectors, and shared cached storage can provide the same portable accessors without native layouts.
///
/// ```compile_fail
/// use base_scheduler::{AddressInfo, AddressInfoCollection, AddressInfoView, SocketType};
/// let entry = {
///     let result = [AddressInfo { address: "127.0.0.1:80".parse().unwrap(), socket_type: SocketType::Stream, protocol: None }];
///     result.entries().next().unwrap()
/// };
/// let address = entry.address(); // A borrowed entry cannot outlive its owner.
/// ```
pub trait AddressInfoCollection: Send + Sync + 'static {
    fn entries(&self) -> impl Iterator<Item: AddressInfoView + Send> + Send;
}

impl<T: AddressInfoView + Send + Sync + 'static> AddressInfoCollection for Vec<T> {
    fn entries(&self) -> impl Iterator<Item: AddressInfoView + Send> + Send {
        self.as_slice().iter()
    }
}

impl<T: AddressInfoView + Send + Sync + 'static, const N: usize> AddressInfoCollection for [T; N] {
    fn entries(&self) -> impl Iterator<Item: AddressInfoView + Send> + Send {
        self.as_slice().iter()
    }
}

impl<T: AddressInfoView + Send + Sync + 'static> AddressInfoCollection for Box<[T]> {
    fn entries(&self) -> impl Iterator<Item: AddressInfoView + Send> + Send {
        self.as_ref().iter()
    }
}
