// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(not(any(unix, windows)))]
compile_error!("base-scheduler-selector currently supports Unix and Windows");

mod blocking_pool;
mod blocking_task;
mod buffer_pooling;
#[cfg(target_os = "linux")]
mod file_extensions;
mod file_io;
mod native_file;
mod native_socket;
#[cfg(unix)]
mod native_subscription;
mod panic;
mod resolver;
#[cfg(unix)]
mod signal_service;
mod socket_io;
mod socket_operations;
mod socket_options;
mod system_address_info;
mod system_addresses;
mod system_file_metadata;
#[cfg(test)]
mod test_support;
#[cfg(unix)]
mod thread;

pub use blocking_task::BlockingTask;
pub use native_file::NativeFile;
pub use native_socket::NativeSocket;
#[cfg(unix)]
pub use native_subscription::NativeSubscription;
pub use system_address_info::SystemAddressInfo;
pub use system_addresses::SystemAddresses;
pub use system_file_metadata::SystemFileMetadata;

mod readiness_driver;
pub use readiness_driver::{ReadinessDriver, ReadinessWorker};

mod readiness_selector;
mod selector;
mod selector_options;
pub use readiness_selector::ReadinessSelector;
pub use selector::Selector;
pub use selector_options::SelectorOptions;

mod selector_driver;
pub use selector_driver::SelectorDriver;

mod selector_worker;
pub use selector_worker::SelectorWorker;

mod socket_receiving;

mod file_state;
mod readiness_file_reader;
mod readiness_file_writer;
pub use readiness_file_reader::ReadinessFileReader;
pub use readiness_file_writer::ReadinessFileWriter;

mod readiness_socket_stream_reader;

mod readiness_socket_packet_reader;

mod readiness_socket_packet_writer;

mod readiness_socket_stream_writer;

mod operation_state;

mod interest;
mod socket_message;
use interest::Interest;
use socket_message::SocketMessage;

#[cfg(test)]
mod bound_io_tests;

mod process_io;
mod process_wait;

#[cfg(test)]
#[test]
#[ignore = "child fixture, invoked by process-wait conformance assertions"]
fn process_child_fixture() {
    base_scheduler_conformance::process_wait::child();
}
