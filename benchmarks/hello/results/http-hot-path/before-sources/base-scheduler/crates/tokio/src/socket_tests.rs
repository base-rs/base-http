// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{TokioOptions, TokioScheduler, TokioSocket};
use base_scheduler::*;
use base_scheduler_conformance::socket;
use futures_lite::future::block_on;
use std::{sync::mpsc, time::Duration};

const TIMEOUT: Duration = Duration::from_secs(20);

fn with_options(options: TokioOptions) -> std::io::Result<TokioScheduler> {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    });
    TokioScheduler::with_options(runtime.handle().clone(), options)
}

fn run(test: impl FnOnce() + Send + 'static) {
    let (done, completion) = mpsc::channel();
    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(test));
        let _ = done.send(result);
    });
    if let Err(payload) = completion
        .recv_timeout(TIMEOUT)
        .expect("socket test deadline exceeded")
    {
        std::panic::resume_unwind(payload);
    }
}

fn scheduler() -> TokioScheduler {
    with_options(TokioOptions {
        threads: 1,
        capacity: 1,
    })
    .unwrap()
}

fn run_stream(
    domain: SocketDomain,
    test: impl AsyncFnOnce(&TokioScheduler, &TokioSocket, &TokioSocket) + Send + 'static,
) {
    run(move || {
        let scheduler = scheduler();
        let (client, server) = block_on(socket::stream_pair(&scheduler, domain));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            block_on(test(&scheduler, &client, &server))
        }));
        block_on(async {
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
    domain: SocketDomain,
    test: impl AsyncFnOnce(&TokioScheduler, &TokioSocket, &TokioSocket) + Send + 'static,
) {
    run(move || {
        let scheduler = scheduler();
        let never = Cancellation::never();
        let local: SocketAddress = match domain {
            SocketDomain::Ipv4 => "127.0.0.1:0",
            SocketDomain::Ipv6 => "[::1]:0",
        }
        .parse()
        .unwrap();
        let (sender, receiver) = block_on(async {
            let sender = scheduler
                .socket_open(
                    domain,
                    SocketType::Datagram,
                    Some(SocketProtocol::Udp),
                    &never,
                )
                .await
                .unwrap();
            let receiver = scheduler
                .socket_open(
                    domain,
                    SocketType::Datagram,
                    Some(SocketProtocol::Udp),
                    &never,
                )
                .await
                .unwrap();
            scheduler.socket_bind(&sender, local, &never).await.unwrap();
            scheduler
                .socket_bind(&receiver, local, &never)
                .await
                .unwrap();
            (sender, receiver)
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            block_on(test(&scheduler, &sender, &receiver))
        }));
        block_on(async {
            if result.is_ok() {
                scheduler.socket_close_submit(sender).await.unwrap();
                scheduler.socket_close(receiver).await.unwrap();
            }
            scheduler.shutdown().await;
        });
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}

mod ipv4 {
    use super::*;
    fn run_stream_case(
        test: impl AsyncFnOnce(&TokioScheduler, &TokioSocket, &TokioSocket) + Send + 'static,
    ) {
        run_stream(SocketDomain::Ipv4, test);
    }
    fn run_datagram_case(
        test: impl AsyncFnOnce(&TokioScheduler, &TokioSocket, &TokioSocket) + Send + 'static,
    ) {
        run_datagram(SocketDomain::Ipv4, test);
    }
    #[test]
    fn pooled_stream_receiver() {
        run_stream_case(async |scheduler, sender, socket| {
            base_scheduler_conformance::socket_receiving::demand_driven(scheduler, sender, socket)
                .await;
            base_scheduler_conformance::socket_receiving::stream(scheduler, sender, socket).await;
        });
    }
    #[test]
    fn pooled_datagram_receiver() {
        run_datagram_case(async |scheduler, sender, socket| {
            base_scheduler_conformance::socket_poll::datagram_first_poll(scheduler, sender, socket)
                .await;
            base_scheduler_conformance::socket_receiving::datagram(scheduler, sender, socket).await;
        });
    }
    mod borrowed_scalar {
        use super::*;
        base_scheduler_conformance::socket_poll_tests!(run: run_stream_case);
    }
    mod borrowed_messages {
        use super::*;
        base_scheduler_conformance::socket_message_poll_tests!(run: run_stream_case);
    }
    mod borrowed_datagrams {
        use super::*;
        base_scheduler_conformance::socket_datagram_poll_tests!(run: run_datagram_case);
    }
    mod packet_handles {
        use super::*;
        base_scheduler_conformance::socket_packet_io_tests!(run: run_datagram_case);
    }
    mod scalar {
        use super::*;
        base_scheduler_conformance::socket_tests!(run: run_stream_case);
    }
    mod messages {
        use super::*;
        base_scheduler_conformance::socket_message_tests!(run: run_stream_case);
    }
    mod datagrams {
        use super::*;
        base_scheduler_conformance::socket_datagram_tests!(run: run_datagram_case);
    }
}

mod ipv6 {
    use super::*;
    fn run_stream_case(
        test: impl AsyncFnOnce(&TokioScheduler, &TokioSocket, &TokioSocket) + Send + 'static,
    ) {
        run_stream(SocketDomain::Ipv6, test);
    }
    fn run_datagram_case(
        test: impl AsyncFnOnce(&TokioScheduler, &TokioSocket, &TokioSocket) + Send + 'static,
    ) {
        run_datagram(SocketDomain::Ipv6, test);
    }
    #[test]
    fn pooled_stream_receiver() {
        run_stream_case(async |scheduler, sender, socket| {
            base_scheduler_conformance::socket_receiving::demand_driven(scheduler, sender, socket)
                .await;
            base_scheduler_conformance::socket_receiving::stream(scheduler, sender, socket).await;
        });
    }
    #[test]
    fn pooled_datagram_receiver() {
        run_datagram_case(async |scheduler, sender, socket| {
            base_scheduler_conformance::socket_poll::datagram_first_poll(scheduler, sender, socket)
                .await;
            base_scheduler_conformance::socket_receiving::datagram(scheduler, sender, socket).await;
        });
    }
    mod borrowed_scalar {
        use super::*;
        base_scheduler_conformance::socket_poll_tests!(run: run_stream_case);
    }
    mod borrowed_messages {
        use super::*;
        base_scheduler_conformance::socket_message_poll_tests!(run: run_stream_case);
    }
    mod borrowed_datagrams {
        use super::*;
        base_scheduler_conformance::socket_datagram_poll_tests!(run: run_datagram_case);
    }
    mod packet_handles {
        use super::*;
        base_scheduler_conformance::socket_packet_io_tests!(run: run_datagram_case);
    }
    mod scalar {
        use super::*;
        base_scheduler_conformance::socket_tests!(run: run_stream_case);
    }
    mod messages {
        use super::*;
        base_scheduler_conformance::socket_message_tests!(run: run_stream_case);
    }
    mod datagrams {
        use super::*;
        base_scheduler_conformance::socket_datagram_tests!(run: run_datagram_case);
    }
}

#[test]
fn sockets_move_between_threads_and_compatible_scheduler_instances() {
    run(|| {
        block_on(async {
            let first = scheduler();
            let second = scheduler();
            let (client, server) = socket::stream_pair(&first, SocketDomain::Ipv4).await;
            std::thread::scope(|scope| {
                let reader = scope.spawn(|| {
                    block_on(async {
                        let mut received = Vec::new();
                        while received.len() < 3 {
                            let (result, buffer) = second
                                .socket_read(
                                    &server,
                                    vec![0; 3 - received.len()],
                                    &Cancellation::never(),
                                )
                                .await;
                            let count = result.unwrap();
                            assert!(count > 0);
                            received.extend_from_slice(&buffer[..count]);
                        }
                        received
                    })
                });
                let mut sent = 0;
                let mut buffer = b"abc".to_vec();
                while sent < 3 {
                    let (result, returned) = block_on(first.socket_write(
                        &client,
                        buffer.slice(sent..),
                        &Cancellation::never(),
                    ));
                    let count = result.unwrap();
                    assert!(count > 0);
                    sent += count;
                    buffer = returned.into_inner();
                }
                assert_eq!(reader.join().unwrap(), b"abc");
            });
            second.socket_close(client).await.unwrap();
            first.socket_close(server).await.unwrap();
            first.shutdown().await;
            second.shutdown().await;
        })
    });
}

#[test]
fn failed_writes_return_the_original_buffer() {
    run(|| {
        block_on(async {
            let scheduler = scheduler();
            let socket = scheduler
                .socket_open(
                    SocketDomain::Ipv4,
                    SocketType::Stream,
                    None,
                    &Cancellation::never(),
                )
                .await
                .unwrap();
            socket::write_error(&scheduler, &socket).await;
            scheduler.socket_close(socket).await.unwrap();
            scheduler.shutdown().await;
        })
    });
}

// Existing callers can keep the aggregate helpers while migrating to generated suites.
#[test]
fn aggregate_socket_helpers_remain_compatible() {
    run_stream(SocketDomain::Ipv4, async |scheduler, sender, receiver| {
        socket::stream(scheduler, sender, receiver).await;
    });
    run_stream(SocketDomain::Ipv4, async |scheduler, sender, receiver| {
        base_scheduler_conformance::socket_messages::stream(scheduler, sender, receiver).await;
    });
    run_datagram(SocketDomain::Ipv4, async |scheduler, sender, receiver| {
        base_scheduler_conformance::socket_messages::datagrams(scheduler, sender, receiver).await;
    });
}

#[test]
fn directional_socket_handles_reuse_caller_owned_storage() {
    run_stream(SocketDomain::Ipv4, async |scheduler, sender, receiver| {
        base_scheduler_conformance::socket_resources::stream(scheduler, sender, receiver).await;
    });
}
