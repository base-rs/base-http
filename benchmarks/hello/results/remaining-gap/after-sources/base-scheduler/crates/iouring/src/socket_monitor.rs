// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use crate::io_uring_selector::{RegisteredSocket, monitor::IoUringMonitorRegistration};
use std::io;

/// Persistent state for one direction of a socket. Create separate readable and writable monitors for concurrent reader and writer tasks.
///
/// `Registration` belongs to the backend. The socket owner outlives every retained registration, including after future abandonment. A monitor does not replace the scheduler explicitly passed to an operation. Await `socket_monitor_close`, then drop the monitor before exclusively closing its socket. Dropping the monitor requests retirement but does not wait for it.
pub(crate) struct SocketMonitor {
    socket: RegisteredSocket,
    interest: Interest,
    registration: IoUringMonitorRegistration,
}

impl SocketMonitor {
    pub fn new(
        socket: RegisteredSocket,
        interest: Interest,
        registration: IoUringMonitorRegistration,
    ) -> Self {
        Self {
            socket,
            interest,
            registration,
        }
    }
    pub fn socket(&self) -> &RegisteredSocket {
        &self.socket
    }
    pub fn interest(&self) -> Interest {
        self.interest
    }
    pub fn registration(&self) -> &IoUringMonitorRegistration {
        &self.registration
    }

    /// Check the direction before attempting I/O.
    pub fn check_interest(&self, interest: Interest) -> io::Result<()> {
        if self.interest == interest {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket monitor has the wrong direction",
            ))
        }
    }
}
