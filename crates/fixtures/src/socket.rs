// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Wait until a TCP peer's kernel has acknowledged everything sent to it, so the peer's next nonblocking read observes that data or, after a write shutdown, EOF. Writing to a socket only queues data for delivery, so without this a test which signals its peer through another channel races the delivery.
//!
//! Linux and macOS query the kernel. Other platforms cannot, so [`is_acknowledged`] sleeps for 50 ms instead, leaving the race unlikely rather than impossible.
//!
//! The kernel queries need `unsafe` system calls. They are confined to this module of an unpublished crate; the published crates forbid `unsafe` code.
#![allow(unsafe_code)]

use std::{
    net::TcpStream,
    thread,
    time::{Duration, Instant},
};

/// How long [`wait_acknowledged`] waits before failing.
pub const ACKNOWLEDGEMENT_TIMEOUT: Duration = Duration::from_secs(5);

/// Wait until the peer has acknowledged every byte written to `stream` and, if writes were shut down, the FIN which followed them, panicking after [`ACKNOWLEDGEMENT_TIMEOUT`].
pub fn wait_acknowledged(stream: &TcpStream) {
    assert!(
        wait_acknowledged_within(stream, ACKNOWLEDGEMENT_TIMEOUT),
        "timed out waiting for the peer to acknowledge"
    );
}

/// Wait up to `timeout` for the peer to acknowledge everything sent on `stream`, returning whether it did.
pub fn wait_acknowledged_within(stream: &TcpStream, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if is_acknowledged(stream) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

/// Whether the peer has acknowledged every byte written to `stream` and any FIN sent after them.
#[cfg(target_os = "linux")]
pub fn is_acknowledged(stream: &TcpStream) -> bool {
    use std::os::fd::AsRawFd;
    let mut unacknowledged: libc::c_int = 0;
    // SAFETY: `stream` keeps the descriptor open, and for a TCP socket `TIOCOUTQ` writes one `c_int` through the pointer.
    let result = unsafe { libc::ioctl(stream.as_raw_fd(), libc::TIOCOUTQ, &mut unacknowledged) };
    succeeded(result);
    // Linux counts the sequence numbers sent but not acknowledged, and a FIN occupies one, so this also covers a write shutdown.
    unacknowledged == 0
}

/// Whether the peer has acknowledged every byte written to `stream` and any FIN sent after them.
#[cfg(target_os = "macos")]
pub fn is_acknowledged(stream: &TcpStream) -> bool {
    use std::os::fd::AsRawFd;
    // The states from `netinet/tcp_fsm.h` in which this side has sent a FIN the peer has not acknowledged.
    const FIN_WAIT_1: u8 = 6;
    const CLOSING: u8 = 7;
    const LAST_ACK: u8 = 8;
    // SAFETY: the structure is plain data, for which all zero bytes are valid.
    let mut connection: libc::tcp_connection_info = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of::<libc::tcp_connection_info>() as libc::socklen_t;
    // SAFETY: `stream` keeps the descriptor open, and the kernel writes at most `length` bytes of `TCP_CONNECTION_INFO` into `connection`.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_CONNECTION_INFO,
            (&raw mut connection).cast(),
            &mut length,
        )
    };
    succeeded(result);
    // The send buffer holds data until the peer acknowledges it, but a FIN only shows in the connection state.
    connection.tcpi_snd_sbbytes == 0
        && !matches!(connection.tcpi_state, FIN_WAIT_1 | CLOSING | LAST_ACK)
}

/// Panic with the operating system's error unless a system call returned zero.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn succeeded(result: libc::c_int) {
    (result == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
        .unwrap();
}

/// Other platforms cannot query acknowledgement, so this waits briefly instead, leaving the race unlikely rather than impossible.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn is_acknowledged(_stream: &TcpStream) -> bool {
    thread::sleep(Duration::from_millis(50));
    true
}
