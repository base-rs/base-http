// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.
mod client;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn main() -> Result<()> {
    let a: Vec<_> = std::env::args().collect();
    assert!(a.len() == 8 && a[1] == "client");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(a[3].parse()?)
        .enable_all()
        .build()?
        .block_on(client::run(
            a[2].parse()?,
            a[4].parse()?,
            a[5].parse()?,
            std::time::Duration::from_secs_f64(a[6].parse()?),
            std::time::Duration::from_secs_f64(a[7].parse()?),
        ))
}
