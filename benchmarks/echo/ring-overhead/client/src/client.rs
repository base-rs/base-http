// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Result;
use hdrhistogram::Histogram;
use std::{
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Barrier, watch},
};

pub(crate) async fn run(
    address: SocketAddr,
    connections: usize,
    bytes: usize,
    warmup: Duration,
    duration: Duration,
) -> Result<()> {
    let gate = Arc::new(Barrier::new(connections + 1));
    let (schedule, clock) = watch::channel(None::<(Instant, Instant)>);
    let mut tasks = Vec::with_capacity(connections);
    for index in 0..connections {
        let mut socket = TcpStream::connect(address)
            .await
            .expect("connect benchmark client");
        socket.set_nodelay(true).expect("TCP_NODELAY");
        let gate = Arc::clone(&gate);
        let mut clock = clock.clone();
        tasks.push(tokio::spawn(async move {
            let request: Vec<u8> = (0..bytes)
                .map(|offset| (offset.wrapping_add(index) % 251) as u8)
                .collect();
            let mut response = vec![0; bytes];
            // Reserve histogram storage before warmup; recording needs no allocation in the timed loop.
            let mut latency = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
                .expect("valid histogram bounds");
            gate.wait().await;
            clock
                .changed()
                .await
                .expect("coordinator publishes schedule");
            let (measure_start, measure_end) =
                clock.borrow().expect("coordinator publishes the schedule");
            loop {
                let started = Instant::now();
                if started >= measure_end {
                    break;
                }
                socket.write_all(&request).await.expect("client write");
                socket.read_exact(&mut response).await.expect("client read");
                let completed = Instant::now();
                if response != request {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "echo payload mismatch",
                    )
                    .into());
                }
                // Count only round trips fully inside the shared measurement window; warmup and boundary-crossing requests are excluded.
                if started >= measure_start && completed <= measure_end {
                    latency
                        .record((completed - started).as_nanos().max(1) as u64)
                        .expect("round trip within histogram bounds");
                }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(latency)
        }));
    }
    gate.wait().await;
    let measure_start = Instant::now() + warmup;
    schedule
        .send(Some((measure_start, measure_start + duration)))
        .expect("clients await schedule");
    let mut latency =
        Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("valid histogram bounds");
    for task in tasks {
        latency
            .add(task.await.expect("client task failed")?)
            .expect("matching histogram bounds");
    }
    if latency.is_empty() {
        return Err("no complete requests in measurement window".into());
    }
    println!(
        "{}",
        serde_json::json!({
            "connections": connections, "bytes": bytes, "warmup_seconds": warmup.as_secs_f64(), "seconds": duration.as_secs_f64(),
            "requests": latency.len(), "requests_per_second": latency.len() as f64 / duration.as_secs_f64(),
            "payload_mib_per_second": latency.len() as f64 * bytes as f64 / duration.as_secs_f64() / 1048576.0,
            "rtt_mean_us": latency.mean() / 1000.0, "rtt_p50_us": latency.value_at_quantile(0.50) as f64 / 1000.0,
            "rtt_p95_us": latency.value_at_quantile(0.95) as f64 / 1000.0, "rtt_p99_us": latency.value_at_quantile(0.99) as f64 / 1000.0,
            "rtt_max_us": latency.max() as f64 / 1000.0,
        })
    );
    Ok(())
}
