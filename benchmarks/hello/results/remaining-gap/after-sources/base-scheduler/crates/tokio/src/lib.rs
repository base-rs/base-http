// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(not(any(unix, windows)))]
compile_error!("base-scheduler-tokio currently supports Unix and Windows");

mod blocking_pool;
mod blocking_task;
mod buffer_pooling;
mod clock;
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
mod system_address_info;
mod system_addresses;
mod system_file_metadata;
mod task_service;
#[cfg(test)]
mod test_support;
#[cfg(unix)]
mod thread;
mod tokio_file;
mod tokio_options;
mod tokio_scheduler;
mod tokio_socket;
#[cfg(unix)]
mod tokio_subscription;
mod tokio_task;

pub use blocking_task::BlockingTask;
pub use system_address_info::SystemAddressInfo;
pub use system_addresses::SystemAddresses;
pub use system_file_metadata::SystemFileMetadata;
pub use tokio_file::TokioFile;
pub use tokio_options::TokioOptions;
pub use tokio_scheduler::TokioScheduler;
pub use tokio_socket::TokioSocket;
#[cfg(unix)]
pub use tokio_subscription::TokioSubscription;
pub use tokio_task::TokioTask;

mod tokio_timer;
pub use tokio_timer::TokioTimer;

mod socket_receiving;

mod tokio_stream_writer;

mod file_state;
mod tokio_file_reader;
mod tokio_file_writer;
pub use tokio_file_reader::TokioFileReader;
pub use tokio_file_writer::TokioFileWriter;

mod tokio_socket_stream_reader;

mod tokio_socket_packet_reader;

mod tokio_socket_packet_writer;

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
