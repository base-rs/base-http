// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{SocketOperation, SocketOption};
use std::{fmt::Debug, io};

/// One supported option and independently observed native state. Use separate getter/setter suites for read-only or write-only capabilities; this fixture requires neither operation by itself.
pub trait SocketOptionFixture {
    type Option: SocketOption<Value: Clone + Debug + PartialEq>;
    type Scheduler: SocketOperation;
    fn scheduler(&self) -> &Self::Scheduler;
    fn socket(&self) -> &<Self::Scheduler as SocketOperation>::Socket;
    /// A resource on which this option fails with `error_kind`, supplied independently of the operation under test.
    fn failing_socket(&self) -> &<Self::Scheduler as SocketOperation>::Socket;
    fn error_kind(&self) -> io::ErrorKind;
    fn option(&self) -> Self::Option;
    /// Two distinct values valid for this socket/option.
    fn values(&self) -> [<Self::Option as SocketOption>::Value; 2];
    fn set_native(&self, value: <Self::Option as SocketOption>::Value);
    fn get_native(&self) -> <Self::Option as SocketOption>::Value;
    /// Get/set syscall attempts, excluding trait-future construction and cancellation checks.
    fn calls(&self) -> usize;
}
