// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Diagnostic event loops: identical borrowed recv/send operations, without tasks.
use io_uring::{IoUring, opcode, types};
use std::{
    io,
    io::Write,
    net::{TcpListener, TcpStream},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

const BUFFER: usize = 65536;
const BATCH: usize = 1024;
const WRITE_TOKEN: u64 = 1 << 32;

struct Connection {
    socket: TcpStream,
    buffer: Box<[u8]>,
    length: usize,
    sent: usize,
    closed: bool,
    write_armed: bool,
    read_ready: bool,
    read_closed: bool,
}

impl Connection {
    // A short stream receive consumes the current readiness hint. Full reads keep probing, matching the measured Base scalar reader policy. Buffers cannot be overwritten until all output has been accepted.
    fn progress(&mut self, readable: bool, closed: bool) -> io::Result<bool> {
        if self.closed {
            return Ok(false);
        }
        self.read_ready |= readable;
        self.read_closed |= closed;
        loop {
            while self.sent < self.length {
                // SAFETY: The live connection exclusively owns the initialized source range for this synchronous call.
                let n = unsafe {
                    libc::send(
                        self.socket.as_raw_fd(),
                        self.buffer[self.sent..self.length].as_ptr().cast(),
                        self.length - self.sent,
                        libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                    )
                };
                if n < 0 {
                    let e = io::Error::last_os_error();
                    match e.kind() {
                        io::ErrorKind::Interrupted => continue,
                        io::ErrorKind::WouldBlock => return Ok(true),
                        _ => return Err(e),
                    }
                }
                if n == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                self.sent += n as usize;
            }
            if !self.read_ready {
                return Ok(false);
            }
            // SAFETY: The live connection exclusively owns the initialized destination for this synchronous call.
            let n = unsafe {
                libc::recv(
                    self.socket.as_raw_fd(),
                    self.buffer.as_mut_ptr().cast(),
                    self.buffer.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            if n < 0 {
                let e = io::Error::last_os_error();
                match e.kind() {
                    io::ErrorKind::Interrupted => continue,
                    io::ErrorKind::WouldBlock => {
                        self.read_ready = false;
                        return Ok(false);
                    }
                    _ => return Err(e),
                }
            }
            if n == 0 {
                self.closed = true;
                return Ok(false);
            }
            self.length = n as usize;
            self.sent = 0;
            self.read_ready = self.length == self.buffer.len() || self.read_closed;
        }
    }
}

fn epoll(connections: &mut [Connection]) -> io::Result<()> {
    // SAFETY: Successful creation transfers a new descriptor to OwnedFd.
    let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let queue = unsafe { OwnedFd::from_raw_fd(fd) };
    for (index, c) in connections.iter().enumerate() {
        let mut event = libc::epoll_event {
            events: (libc::EPOLLIN
                | libc::EPOLLOUT
                | libc::EPOLLPRI
                | libc::EPOLLRDHUP
                | libc::EPOLLET) as u32,
            u64: index as u64,
        };
        // SAFETY: Descriptors and event storage remain valid through the call. Connections outlive queue registration.
        if unsafe {
            libc::epoll_ctl(
                queue.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                c.socket.as_raw_fd(),
                &mut event,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; BATCH];
    let mut remaining = connections.len();
    while remaining > 0 {
        // SAFETY: The result vector has capacity for BATCH initialized events.
        let n =
            unsafe { libc::epoll_wait(queue.as_raw_fd(), events.as_mut_ptr(), BATCH as i32, -1) };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        for e in &events[..n as usize] {
            let c = &mut connections[e.u64 as usize];
            let readable = e.events
                & (libc::EPOLLIN | libc::EPOLLRDHUP | libc::EPOLLHUP | libc::EPOLLERR) as u32
                != 0;
            let was_closed = c.closed;
            c.progress(
                readable,
                e.events & (libc::EPOLLRDHUP | libc::EPOLLHUP | libc::EPOLLERR) as u32 != 0,
            )?;
            if c.closed && !was_closed {
                remaining -= 1;
                // SAFETY: The socket is still owned; deletion removes any future interest.
                if unsafe {
                    libc::epoll_ctl(
                        queue.as_raw_fd(),
                        libc::EPOLL_CTL_DEL,
                        c.socket.as_raw_fd(),
                        std::ptr::null_mut(),
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
        }
    }
    Ok(())
}

fn push(ring: &mut IoUring, entry: io_uring::squeue::Entry) -> io::Result<()> {
    if ring.submission().is_full() {
        ring.submit()?;
    }
    // SAFETY: Poll entries contain only owned live descriptors and opaque tokens, with no borrowed payload pointers. The ring is destroyed before the connections.
    unsafe { ring.submission().push(&entry) }.map_err(|_| io::Error::other("submission queue full"))
}

fn uring(connections: &mut [Connection], mode: &str) -> io::Result<()> {
    let mut builder = IoUring::builder();
    if mode == "uring" {
        builder.setup_coop_taskrun().setup_taskrun_flag();
    }
    if mode == "uring-defer" {
        builder
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_taskrun_flag();
    }
    let mut ring = builder.build(256)?;
    for (index, c) in connections.iter().enumerate() {
        push(
            &mut ring,
            opcode::PollAdd::new(
                types::Fd(c.socket.as_raw_fd()),
                (libc::POLLIN | libc::POLLPRI | libc::POLLRDHUP) as u32,
            )
            .multi(true)
            .build()
            .user_data(index as u64),
        )?;
    }
    let mut events = Vec::with_capacity(BATCH);
    let mut remaining = connections.len();
    while remaining > 0 {
        let (pending, taskrun, overflow) = {
            let sq = ring.submission();
            (!sq.is_empty(), sq.taskrun(), sq.cq_overflow())
        };
        let empty = ring.completion().is_empty();
        if empty {
            match ring.submit_and_wait(1) {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                r => {
                    r?;
                }
            }
        } else if pending || taskrun || overflow {
            ring.submit()?;
        }
        events.clear();
        for cqe in ring.completion().take(BATCH) {
            events.push((
                cqe.user_data(),
                cqe.result(),
                io_uring::cqueue::more(cqe.flags()),
            ));
        }
        for &(token, result, more) in &events {
            let c = &mut connections[(token & !WRITE_TOKEN) as usize];
            if c.closed {
                continue;
            }
            if result < 0 {
                return Err(io::Error::from_raw_os_error(-result));
            }
            if token & WRITE_TOKEN != 0 {
                c.write_armed = false;
            }
            let blocked = c.progress(
                token & WRITE_TOKEN == 0,
                result & (libc::POLLRDHUP | libc::POLLHUP | libc::POLLERR) as i32 != 0,
            )?;
            if c.closed {
                remaining -= 1;
            }
            if !c.closed && token & WRITE_TOKEN == 0 && !more {
                return Err(io::Error::other("multishot read poll terminated"));
            }
            if blocked && !c.write_armed {
                push(
                    &mut ring,
                    opcode::PollAdd::new(types::Fd(c.socket.as_raw_fd()), libc::POLLOUT as u32)
                        .build()
                        .user_data((token & !WRITE_TOKEN) | WRITE_TOKEN),
                )?;
                c.write_armed = true;
            }
        }
    }
    // Closing the ring retires its poll registrations before connections are released. Poll requests access no application payload memory.
    drop(ring);
    Ok(())
}

fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() == 5 && args[1] == "server" && args[3] == "1",
        "server BACKEND 1 CONNECTIONS"
    );
    let count: usize = args[4].parse().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0")?;
    println!("{}", listener.local_addr()?);
    io::stdout().flush()?;
    let mut connections = Vec::with_capacity(count);
    for _ in 0..count {
        let (socket, _) = listener.accept()?;
        socket.set_nodelay(true)?;
        socket.set_nonblocking(true)?;
        connections.push(Connection {
            socket,
            buffer: vec![0; BUFFER].into_boxed_slice(),
            length: 0,
            sent: 0,
            closed: false,
            write_armed: false,
            read_ready: false,
            read_closed: false,
        });
    }
    match args[2].as_str() {
        "epoll" => epoll(&mut connections),
        mode @ ("uring" | "uring-default" | "uring-defer") => uring(&mut connections, mode),
        _ => panic!("unknown backend"),
    }
}
