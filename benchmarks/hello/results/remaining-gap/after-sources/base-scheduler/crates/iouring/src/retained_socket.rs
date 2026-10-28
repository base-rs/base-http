// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{SocketRecvFlags, SocketRecvResult, SocketSendFlags};
use base_scheduler_selector::NativeSocket;
use socket2::{SockAddr, Socket};
use std::{
    io::{self, IoSlice, IoSliceMut},
    net::SocketAddr,
    os::fd::AsRawFd,
    sync::Arc,
};

type RegisteredSocket = Arc<async_io::Async<Socket>>;

// Factories and queued requests can retain either a native endpoint or an already-retained registration.
pub(crate) trait SocketSource: Send + Sync {
    fn retain_socket(&self) -> RegisteredSocket;
}
impl SocketSource for NativeSocket {
    fn retain_socket(&self) -> RegisteredSocket {
        // SAFETY: Backend state retains the owner through registration and operation retirement, without changing its descriptor or nonblocking mode.
        unsafe { self.retain_native() }
    }
}
impl SocketSource for RegisteredSocket {
    fn retain_socket(&self) -> RegisteredSocket {
        Arc::clone(self)
    }
}
#[inline]
pub(crate) fn recv(
    socket: &RegisteredSocket,
    bytes: &mut [u8],
    flags: SocketRecvFlags,
) -> io::Result<usize> {
    // SAFETY: The mutable initialized range remains valid through this synchronous call. MSG_DONTWAIT never extends payload access beyond return.
    result_count(unsafe {
        libc::recv(
            socket.as_raw_fd(),
            bytes.as_mut_ptr().cast(),
            bytes.len(),
            libc::MSG_DONTWAIT | recv_flags(flags),
        )
    })
}
#[inline]
pub(crate) fn send(
    socket: &RegisteredSocket,
    bytes: &[u8],
    flags: SocketSendFlags,
) -> io::Result<usize> {
    // SAFETY: The readable range remains valid through this synchronous call. MSG_DONTWAIT never retains the payload beyond return.
    result_count(unsafe {
        libc::send(
            socket.as_raw_fd(),
            bytes.as_ptr().cast(),
            bytes.len(),
            libc::MSG_DONTWAIT | send_flags(flags),
        )
    })
}
#[inline]
pub(crate) fn recvmsg(
    socket: &RegisteredSocket,
    buffers: &mut [IoSliceMut<'_>],
    flags: SocketRecvFlags,
) -> io::Result<SocketRecvResult> {
    validate(buffers.len(), buffers.iter().map(|buffer| buffer.len()))?;
    let empty = libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    };
    let (vectors, count) = if buffers.is_empty() {
        (&empty as *const libc::iovec, 1)
    } else {
        (buffers.as_ptr().cast(), buffers.len())
    };
    // SAFETY: IoSliceMut is ABI-compatible with iovec; header and exclusive payload pointers remain live through recvmsg and are not retained afterward.
    let ((bytes, truncated, control_truncated), address) = unsafe {
        SockAddr::try_init(|address, length| {
            let mut header: libc::msghdr = std::mem::zeroed();
            header.msg_iov = vectors.cast_mut();
            header.msg_iovlen = count;
            header.msg_name = address.cast();
            header.msg_namelen = *length;
            let bytes = result_count(libc::recvmsg(
                socket.as_raw_fd(),
                &mut header,
                libc::MSG_DONTWAIT | recv_flags(flags),
            ))?;
            *length = header.msg_namelen;
            Ok((
                bytes,
                header.msg_flags & libc::MSG_TRUNC != 0,
                header.msg_flags & libc::MSG_CTRUNC != 0,
            ))
        })
    }?;
    Ok(SocketRecvResult {
        bytes,
        address: address.as_socket(),
        truncated,
        control_truncated,
    })
}
#[inline]
pub(crate) fn sendmsg(
    socket: &RegisteredSocket,
    buffers: &[IoSlice<'_>],
    address: Option<SocketAddr>,
    flags: SocketSendFlags,
) -> io::Result<usize> {
    validate(buffers.len(), buffers.iter().map(|buffer| buffer.len()))?;
    let empty = libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    };
    let (vectors, count) = if buffers.is_empty() {
        (&empty as *const libc::iovec, 1)
    } else {
        (buffers.as_ptr().cast(), buffers.len())
    };
    let address = address.map(SockAddr::from);
    // SAFETY: IoSlice is ABI-compatible with iovec. Header, address and readable payloads remain live for this synchronous sendmsg only.
    let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
    header.msg_iov = vectors.cast_mut();
    header.msg_iovlen = count;
    if let Some(address) = &address {
        header.msg_name = address.as_ptr().cast_mut().cast();
        header.msg_namelen = address.len();
    }
    // SAFETY: All header pointers refer to the live values above; nonblocking send does not retain them after return.
    result_count(unsafe {
        libc::sendmsg(
            socket.as_raw_fd(),
            &header,
            libc::MSG_DONTWAIT | send_flags(flags),
        )
    })
}
fn validate(count: usize, mut lengths: impl Iterator<Item = usize>) -> io::Result<()> {
    if count > i32::MAX as usize
        || lengths
            .try_fold(0usize, usize::checked_add)
            .is_none_or(|length| length > i32::MAX as usize)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "message exceeds native length limits",
        ));
    }
    Ok(())
}
fn send_flags(flags: SocketSendFlags) -> i32 {
    libc::MSG_NOSIGNAL
        | if flags.dont_route {
            libc::MSG_DONTROUTE
        } else {
            0
        }
}
fn recv_flags(flags: SocketRecvFlags) -> i32 {
    if flags.peek { libc::MSG_PEEK } else { 0 }
}
#[inline]
fn result_count(result: isize) -> io::Result<usize> {
    if result >= 0 {
        Ok(result as usize)
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(test, target_pointer_width = "64"))]
mod tests;
