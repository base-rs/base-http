// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! How the server treats a placement that fails as a scoped spawn can: refusing to spawn ends serving, a connection destroyed before completing is not a failure, and one that panicked is.
use base_http_fixtures::Hello;
use base_http_server::{Endpoint, Error, Options, Server, Statistics};
use base_scheduler::{
    Cancellation, Placement, SocketBind, SocketListen, SocketLocalAddress, SocketOpen, SocketType,
    SpawnError, TaskError,
};
use base_scheduler_native::NativeScheduler;
use futures_lite::future::block_on;
use std::{error::Error as _, future::Future, io::Read, net::SocketAddr, net::TcpStream};

#[derive(Clone, Copy)]
enum Fault {
    Refuse,
    Destroy,
    Panic,
}

// Places nothing: it refuses, or reports the work destroyed or panicked without running it.
#[derive(Clone, Copy)]
struct Faulty<'a> {
    scheduler: &'a NativeScheduler,
    fault: Fault,
}

impl<'a> Placement<'a> for Faulty<'a> {
    type Scheduler = NativeScheduler;

    fn scheduler(self) -> &'a NativeScheduler {
        self.scheduler
    }

    fn place<F>(
        self,
        work: F,
    ) -> Result<impl Future<Output = Result<F::Output, TaskError>> + Send + 'a, SpawnError>
    where
        F: Future + Send + 'a,
        F::Output: Send + 'a,
    {
        let fault = self.fault;
        if let Fault::Refuse = fault {
            return Err(SpawnError::SchedulerClosed);
        }
        Ok(async move {
            drop(work);
            Err(match fault {
                Fault::Panic => TaskError::Panicked(Box::new("faulty")),
                _ => TaskError::Cancelled,
            })
        })
    }
}

// Serve one peer through `fault`, cancelling once the peer sees its connection closed.
fn serve(fault: Fault) -> Result<Statistics, Error> {
    let scheduler = NativeScheduler::new().unwrap();
    let options = Options::default();
    let cancellation = Cancellation::new();
    let result = block_on(async {
        let never = Cancellation::never();
        let address: SocketAddr = ([127, 0, 0, 1], 0).into();
        let listener = scheduler
            .socket_open(address.into(), SocketType::Stream, None, &never)
            .await
            .unwrap();
        scheduler
            .socket_bind(&listener, address, &never)
            .await
            .unwrap();
        scheduler
            .socket_listen(&listener, 16, &never)
            .await
            .unwrap();
        let address = scheduler.socket_local_address(&listener).unwrap();
        let peer = {
            let cancellation = cancellation.clone();
            std::thread::spawn(move || {
                let mut stream = TcpStream::connect(address).unwrap();
                // The connection is dropped without being served.
                let _ = stream.read(&mut [0; 1]);
                cancellation.cancel();
            })
        };
        let placement = Faulty {
            scheduler: &scheduler,
            fault,
        };
        let result = Server::new(&Hello, Endpoint::new(listener), &options)
            .run(placement, &cancellation)
            .await;
        if let Fault::Refuse = fault {
            // Serving stopped, so nothing wakes the peer; end it here.
            cancellation.cancel();
        }
        peer.join().unwrap();
        result
    });
    block_on(scheduler.shutdown());
    result
}

#[test]
fn a_refused_spawn_ends_serving() {
    let error = serve(Fault::Refuse).unwrap_err();
    assert!(
        matches!(error, Error::Spawn(SpawnError::SchedulerClosed)),
        "{error:?}"
    );
    assert_eq!(error.to_string(), "HTTP connection could not be spawned");
    assert_eq!(
        error.source().unwrap().to_string(),
        SpawnError::SchedulerClosed.to_string()
    );
}

#[test]
fn a_destroyed_connection_is_not_a_failure() {
    let statistics = serve(Fault::Destroy).unwrap();
    assert_eq!(statistics.connections, 1);
    assert_eq!(statistics.failed_connections, 0);
}

#[test]
fn a_panicked_connection_is_a_failure() {
    let statistics = serve(Fault::Panic).unwrap();
    assert_eq!(statistics.connections, 1);
    assert_eq!(statistics.failed_connections, 1);
}
