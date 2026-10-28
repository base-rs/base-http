// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![deny(unsafe_op_in_unsafe_fn)]
#![cfg(target_os = "linux")]

mod fallback;
mod io_uring_driver;
mod io_uring_options;
mod io_uring_selector;
pub use io_uring_driver::IoUringDriver;
pub use io_uring_options::IoUringOptions;
pub use io_uring_selector::IoUringSelector;

pub use io_uring_selector::multishot::{
    RecvMsgBuffer, RecvMsgMultishot, RecvMsgMultishotStatistics,
};

mod io_uring_worker;
pub use io_uring_worker::IoUringWorker;

mod io_uring_buffer;
mod io_uring_buffer_pool;
pub use io_uring_buffer::IoUringBuffer;
pub use io_uring_buffer_pool::IoUringBufferPool;

mod io_uring_recv_message;
pub use io_uring_recv_message::IoUringRecvMessage;

mod recv_msg_multishot_options;
pub use recv_msg_multishot_options::RecvMsgMultishotOptions;

mod monitor_storage;
mod operation_state;
mod socket_monitor;
use monitor_storage::MonitorStorage;
use socket_monitor::SocketMonitor;
mod retained_socket;

mod io_uring_socket_packet_reader;
mod io_uring_socket_packet_writer;
mod io_uring_socket_stream_reader;
mod io_uring_socket_stream_writer;

mod socket_resources;

mod io_uring_file_reader;
mod io_uring_file_writer;
pub use io_uring_file_reader::IoUringFileReader;
pub use io_uring_file_writer::IoUringFileWriter;

mod interest;
mod socket_message;
use interest::Interest;
use socket_message::SocketMessage;

#[cfg(all(test, target_pointer_width = "64"))]
mod test_buffers;
