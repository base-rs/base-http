// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(not(any(unix, windows)))]
compile_error!("base-scheduler-direct currently supports Unix and Windows");

mod blocking_pool;
mod blocking_task;
mod buffer_pooling;
mod direct_file;
mod direct_options;
mod direct_scheduler;
mod direct_socket;
#[cfg(unix)]
mod direct_subscription;
mod direct_task;
#[cfg(target_os = "linux")]
mod file_extensions;
mod file_io;
mod panic;
mod resolver;
#[cfg(unix)]
mod signal_service;
mod socket_io;
mod socket_operations;
mod socket_options;
mod socket_retry;
mod system_address_info;
mod system_addresses;
mod system_file_metadata;
mod task_service;
#[cfg(test)]
mod test_support;
mod thread;
mod timer_service;

pub use blocking_task::BlockingTask;
pub use direct_file::DirectFile;
pub use direct_options::DirectOptions;
pub use direct_scheduler::DirectScheduler;
pub use direct_socket::DirectSocket;
#[cfg(unix)]
pub use direct_subscription::DirectSubscription;
pub use direct_task::DirectTask;
pub use system_address_info::SystemAddressInfo;
pub use system_addresses::SystemAddresses;
pub use system_file_metadata::SystemFileMetadata;

pub use timer_service::DirectTimer;

mod socket_receiving;

mod direct_stream_writer;

mod direct_file_reader;
mod direct_file_writer;
mod file_state;
pub use direct_file_reader::DirectFileReader;
pub use direct_file_writer::DirectFileWriter;

mod direct_socket_stream_reader;

mod direct_socket_packet_reader;

mod direct_socket_packet_writer;

mod socket_write_state;

#[cfg(test)]
mod socket_tests;

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
