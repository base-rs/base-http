// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::*;
use base_scheduler_conformance::{FileTestFixture, socket};
use base_scheduler_tokio::{TokioScheduler, TokioSocket};

fn run(test: impl FnOnce() + Send + 'static) {
    let (done, completed) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(test)));
    });
    if let Err(payload) = completed
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("runtime conformance deadline exceeded")
    {
        std::panic::resume_unwind(payload);
    }
}
fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn multi_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
struct FileFixture {
    scheduler: TokioScheduler,
    path: std::path::PathBuf,
}
impl FileTestFixture for FileFixture {
    type Scheduler = TokioScheduler;
    fn scheduler(&self) -> &TokioScheduler {
        &self.scheduler
    }
    fn path(&self) -> &std::path::Path {
        &self.path
    }
    fn assert_absent(&self) {
        assert!(!self.path.exists());
    }
    fn assert_contents(&self, expected: &[u8]) {
        assert_eq!(std::fs::read(&self.path).unwrap(), expected);
    }
}
fn run_file(
    build: fn() -> tokio::runtime::Runtime,
    test: impl AsyncFnOnce(&FileFixture) + Send + 'static,
) {
    run(move || {
        let runtime = build();
        let directory = tempfile::tempdir().unwrap();
        let fixture = FileFixture {
            scheduler: TokioScheduler::new(runtime.handle().clone()),
            path: directory.path().join("file"),
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(test(&fixture))
        }));
        runtime.block_on(fixture.scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
fn run_stream(
    build: fn() -> tokio::runtime::Runtime,
    domain: SocketDomain,
    test: impl AsyncFnOnce(&TokioScheduler, &TokioSocket, &TokioSocket) + Send + 'static,
) {
    run(move || {
        let runtime = build();
        let scheduler = TokioScheduler::new(runtime.handle().clone());
        let (client, server) = runtime.block_on(socket::stream_pair(&scheduler, domain));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(test(&scheduler, &client, &server))
        }));
        runtime.block_on(async {
            if result.is_ok() {
                scheduler.socket_close(client).await.unwrap();
                scheduler.socket_close(server).await.unwrap();
            }
            scheduler.shutdown().await;
        });
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
fn run_datagram(
    build: fn() -> tokio::runtime::Runtime,
    domain: SocketDomain,
    test: impl AsyncFnOnce(&TokioScheduler, &TokioSocket, &TokioSocket) + Send + 'static,
) {
    run(move || {
        let runtime = build();
        let scheduler = TokioScheduler::new(runtime.handle().clone());
        let (first, second) = runtime.block_on(async {
            let token = Cancellation::never();
            let first = scheduler
                .socket_open(
                    domain,
                    SocketType::Datagram,
                    Some(SocketProtocol::Udp),
                    &token,
                )
                .await
                .unwrap();
            let second = scheduler
                .socket_open(
                    domain,
                    SocketType::Datagram,
                    Some(SocketProtocol::Udp),
                    &token,
                )
                .await
                .unwrap();
            let address = match domain {
                SocketDomain::Ipv4 => "127.0.0.1:0",
                SocketDomain::Ipv6 => "[::1]:0",
            }
            .parse()
            .unwrap();
            scheduler
                .socket_bind(&first, address, &token)
                .await
                .unwrap();
            scheduler
                .socket_bind(&second, address, &token)
                .await
                .unwrap();
            (first, second)
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(test(&scheduler, &first, &second))
        }));
        runtime.block_on(scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
macro_rules! suites {
    ($name:ident, $build:ident) => {
        mod $name {
            use super::*;
            fn file_case(test: impl AsyncFnOnce(&FileFixture) + Send + 'static) { run_file($build, test); }
            mod files { base_scheduler_conformance::file_tests!(run: super::file_case); }
            macro_rules! sockets {
                ($family:ident, $domain:ident) => {
                    mod $family {
                        use super::*;
                        fn stream(test: impl AsyncFnOnce(&TokioScheduler, &TokioSocket, &TokioSocket) + Send + 'static) { run_stream($build, SocketDomain::$domain, test); }
                        fn datagram(test: impl AsyncFnOnce(&TokioScheduler, &TokioSocket, &TokioSocket) + Send + 'static) { run_datagram($build, SocketDomain::$domain, test); }
                        mod scalar { base_scheduler_conformance::socket_tests!(run: super::stream); }
                        mod messages { base_scheduler_conformance::socket_message_tests!(run: super::stream); }
                        mod datagrams { base_scheduler_conformance::socket_datagram_tests!(run: super::datagram); }
                    }
                };
            }
            sockets!(ipv4, Ipv4);
            sockets!(ipv6, Ipv6);
        }
    };
}
suites!(single_thread, current_thread);
suites!(multiple_threads, multi_thread);
