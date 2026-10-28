// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Linux fixed-arrival raw HTTP client. One outstanding request per connection;
//! an absolute per-connection schedule continues accumulating while blocked.
//! Busy polling avoids millisecond timer quantization. Never use this for
//! production traffic: it intentionally consumes each configured client CPU.

use hdrhistogram::Histogram;
use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::{Arc, Barrier, OnceLock},
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nHello, world!";

fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()
}
fn offset(ordinal: u64, rate: u64) -> Duration {
    Duration::from_nanos((u128::from(ordinal) * 1_000_000_000 / u128::from(rate)) as u64)
}
fn pin(cpu: usize) -> io::Result<()> {
    // SAFETY: The initialized set is valid for the calling thread. CPU_SET's
    // index is checked against the fixed cpu_set_t capacity by our caller.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

struct Active {
    planned: Instant,
    sent: Instant,
    eligible: Instant,
    received: usize,
}
struct Connection {
    stream: TcpStream,
    ordinal: u64,
    available: Instant,
    active: Option<Active>,
    response: [u8; RESPONSE.len()],
}
struct Counts {
    scheduled: Histogram<u64>,
    wire: Histogram<u64>,
    send_delay: Histogram<u64>,
    dispatch: Histogram<u64>,
    per_second_latency: Vec<Histogram<u64>>,
    sent_in_window: u64,
    completed_in_window: u64,
    completed_per_second: Vec<u64>,
    total_completed: u64,
    write_would_block: u64,
    last_completion: Instant,
}
impl Counts {
    fn new(seconds: usize) -> Self {
        Self {
            scheduled: histogram(),
            wire: histogram(),
            send_delay: histogram(),
            dispatch: histogram(),
            per_second_latency: (0..seconds).map(|_| histogram()).collect(),
            sent_in_window: 0,
            completed_in_window: 0,
            completed_per_second: vec![0; seconds],
            total_completed: 0,
            write_would_block: 0,
            last_completion: Instant::now(),
        }
    }
}
fn record(histogram: &mut Histogram<u64>, duration: Duration) -> Result<()> {
    histogram.record((duration.as_nanos() as u64).max(1))?;
    Ok(())
}

struct Config {
    address: SocketAddr,
    connections: usize,
    rate: u64,
    warmup: u64,
    seconds: u64,
    cpus: Vec<usize>,
    workers_ready: Barrier,
    start: OnceLock<Instant>,
}

fn worker(config: Arc<Config>, worker_index: usize) -> Result<Counts> {
    pin(config.cpus[worker_index])?;
    let workers = config.cpus.len();
    // SAFETY: epoll_create1 returns a new descriptor, transferred to OwnedFd.
    let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let epoll = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut connections = Vec::new();
    for global_index in (worker_index..config.connections).step_by(workers) {
        let stream = TcpStream::connect(config.address)?;
        stream.set_nodelay(true)?;
        stream.set_nonblocking(true)?;
        let mut event = libc::epoll_event {
            events: (libc::EPOLLIN | libc::EPOLLRDHUP) as u32,
            u64: connections.len() as u64,
        };
        // SAFETY: Both descriptors are live; epoll copies the event before return.
        if unsafe {
            libc::epoll_ctl(
                epoll.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                stream.as_raw_fd(),
                &mut event,
            )
        } != 0
        {
            return Err(io::Error::last_os_error().into());
        }
        connections.push(Connection {
            stream,
            ordinal: global_index as u64,
            available: Instant::now(),
            active: None,
            response: [0; RESPONSE.len()],
        });
    }
    let request = format!("GET / HTTP/1.1\r\nHost: {}\r\n\r\n", config.address).into_bytes();
    let mut counts = Counts::new(config.seconds as usize);
    config.workers_ready.wait();
    let start = loop {
        if let Some(start) = config.start.get() {
            break *start;
        }
        std::hint::spin_loop();
    };
    let measure_start = start + Duration::from_secs(config.warmup);
    let measure_end = measure_start + Duration::from_secs(config.seconds);
    let deadline = measure_end + Duration::from_secs(15);
    let offered = config.rate * (config.warmup + config.seconds);
    let mut events: [libc::epoll_event; 64] = unsafe { std::mem::zeroed() };
    loop {
        // SAFETY: The writable event array has the advertised capacity and the
        // descriptor stays live through the call. Zero timeout never sleeps.
        let n = unsafe {
            libc::epoll_wait(
                epoll.as_raw_fd(),
                events.as_mut_ptr(),
                events.len() as i32,
                0,
            )
        };
        if n < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error.into());
        }
        for event in &events[..n as usize] {
            let index = event.u64 as usize;
            let connection = &mut connections[index];
            let Some(active) = &mut connection.active else {
                return Err(io::Error::other(
                    "unexpected response or connection closure while idle",
                )
                .into());
            };
            loop {
                match connection
                    .stream
                    .read(&mut connection.response[active.received..])
                {
                    Ok(0) => {
                        return Err(
                            io::Error::new(io::ErrorKind::UnexpectedEof, "response EOF").into()
                        );
                    }
                    Ok(n) => active.received += n,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error.into()),
                }
                if active.received == RESPONSE.len() {
                    let completed = Instant::now();
                    if connection.response != RESPONSE {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "fixed response mismatch",
                        )
                        .into());
                    }
                    if completed >= measure_start && completed < measure_end {
                        counts.completed_in_window += 1;
                        counts.completed_per_second
                            [(completed - measure_start).as_secs() as usize] += 1;
                    }
                    if active.planned >= measure_start {
                        record(&mut counts.scheduled, completed - active.planned)?;
                        record(&mut counts.wire, completed - active.sent)?;
                        record(&mut counts.send_delay, active.sent - active.planned)?;
                        record(&mut counts.dispatch, active.sent - active.eligible)?;
                        record(
                            &mut counts.per_second_latency
                                [(active.planned - measure_start).as_secs() as usize],
                            completed - active.planned,
                        )?;
                    }
                    counts.total_completed += 1;
                    counts.last_completion = completed;
                    connection.available = completed;
                    connection.active = None;
                    connection.ordinal += config.connections as u64;
                    break;
                }
            }
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "fixed arrival backlog did not drain within 15 seconds",
            )
            .into());
        }
        let mut remaining = false;
        for connection in &mut connections {
            if connection.ordinal >= offered {
                continue;
            }
            remaining = true;
            if connection.active.is_some() {
                continue;
            }
            let planned = start + offset(connection.ordinal, config.rate);
            if now < planned {
                continue;
            }
            let sent = Instant::now();
            match connection.stream.write(&request) {
                Ok(n) if n == request.len() => {}
                Ok(_) => {
                    return Err(
                        io::Error::other("raw control requires one complete request write").into(),
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    counts.write_would_block += 1;
                    continue;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
            if sent >= measure_start && sent < measure_end {
                counts.sent_in_window += 1;
            }
            connection.active = Some(Active {
                planned,
                sent,
                eligible: planned.max(connection.available),
                received: 0,
            });
        }
        if !remaining {
            break;
        }
        std::hint::spin_loop();
    }
    Ok(counts)
}

fn describe(histogram: &Histogram<u64>) -> serde_json::Value {
    serde_json::json!({
        "count":histogram.len(), "mean_us":histogram.mean()/1000.0,
        "p50_us":histogram.value_at_quantile(0.5) as f64/1000.0,
        "p95_us":histogram.value_at_quantile(0.95) as f64/1000.0,
        "p99_us":histogram.value_at_quantile(0.99) as f64/1000.0,
        "p999_us":histogram.value_at_quantile(0.999) as f64/1000.0,
        "max_us":histogram.max() as f64/1000.0
    })
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 7 {
        return Err(
            "usage: CLIENT ADDRESS CONNECTIONS RATE WARMUP_SECONDS SECONDS CPU,CPU,...".into(),
        );
    }
    let cpus: Vec<usize> = args[6]
        .split(',')
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()?;
    let connections: usize = args[2].parse()?;
    let rate: u64 = args[3].parse()?;
    let warmup: u64 = args[4].parse()?;
    let seconds: u64 = args[5].parse()?;
    if cpus.is_empty()
        || cpus.len() > connections
        || cpus.iter().any(|&c| c >= libc::CPU_SETSIZE as usize)
        || connections == 0
        || connections > 1024
        || rate == 0
        || seconds == 0
        || warmup + seconds > 60
    {
        return Err("invalid configuration".into());
    }
    let config = Arc::new(Config {
        address: args[1].parse()?,
        connections,
        rate,
        warmup,
        seconds,
        workers_ready: Barrier::new(cpus.len() + 1),
        start: OnceLock::new(),
        cpus,
    });
    let tasks: Vec<_> = (0..config.cpus.len())
        .map(|index| {
            let config = Arc::clone(&config);
            std::thread::spawn(move || worker(config, index))
        })
        .collect();
    config.workers_ready.wait();
    let start = Instant::now() + Duration::from_millis(100);
    config.start.set(start).unwrap();
    let mut total = Counts::new(seconds as usize);
    for task in tasks {
        let counts = task
            .join()
            .map_err(|_| io::Error::other("client worker panicked"))??;
        total.scheduled.add(&counts.scheduled)?;
        total.wire.add(&counts.wire)?;
        total.send_delay.add(&counts.send_delay)?;
        total.dispatch.add(&counts.dispatch)?;
        for (a, b) in total
            .per_second_latency
            .iter_mut()
            .zip(&counts.per_second_latency)
        {
            a.add(b)?;
        }
        for (a, b) in total
            .completed_per_second
            .iter_mut()
            .zip(&counts.completed_per_second)
        {
            *a += b;
        }
        total.sent_in_window += counts.sent_in_window;
        total.completed_in_window += counts.completed_in_window;
        total.total_completed += counts.total_completed;
        total.write_would_block += counts.write_would_block;
        total.last_completion = total.last_completion.max(counts.last_completion);
    }
    assert_eq!(total.total_completed, rate * (warmup + seconds));
    assert_eq!(total.scheduled.len(), rate * seconds);
    println!(
        "{}",
        serde_json::json!({
            "connections":connections,"rate":rate,"warmup_seconds":warmup,"seconds":seconds,"cpus":config.cpus,
            "planned_measured_requests":rate*seconds,"measured_responses_validated":total.scheduled.len(),
            "total_responses_validated":total.total_completed,"sent_in_window":total.sent_in_window,
            "completed_in_window":total.completed_in_window,"achieved_rate":total.completed_in_window as f64/seconds as f64,
            "send_rate":total.sent_in_window as f64/seconds as f64,"completed_per_second":total.completed_per_second,
            "scheduled_latency":describe(&total.scheduled),"wire_rtt":describe(&total.wire),
            "send_delay":describe(&total.send_delay),"dispatch_lag":describe(&total.dispatch),
            "per_second_scheduled_latency":total.per_second_latency.iter().map(describe).collect::<Vec<_>>(),
            "drain_ms":total.last_completion.saturating_duration_since(start+Duration::from_secs(warmup+seconds)).as_secs_f64()*1000.0,
            "write_would_block":total.write_would_block,"payload_validation_errors":0
        })
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schedule_has_no_cumulative_rounding_drift() {
        assert_eq!(offset(240_000, 240_000), Duration::from_secs(1));
        assert_eq!(offset(64, 240_000), Duration::from_nanos(266_666));
        assert_eq!(offset(48_000_000, 240_000), Duration::from_secs(200));
    }
}
