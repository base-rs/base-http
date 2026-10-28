//! Disposable microbenchmark hooks; never part of production scheduler builds.
use super::*;
use std::{future::Future, hint::black_box, pin::pin, task::Context, time::Instant};

pub fn probe(kind: &str, iterations: u64, width: usize, waker: &Waker) -> (u64, usize) {
    let (group, drivers) = create(1, Cancellation::new()).unwrap();
    let group = group.unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let directions: Vec<_> = (0..width).map(|_| Direction::new()).collect();
    // Warm listener storage before timing.
    for direction in &directions {
        let mut waiting = pin!(direction.wait(&group));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        direction.publish();
        assert!(waiting.as_mut().poll(&mut cx).is_ready());
        direction.clear(direction.snapshot());
    }
    let mut cx = Context::from_waker(waker);
    let result = match kind {
        "readiness" => {
            let start = Instant::now();
            for _ in 0..iterations {
                for direction in &directions {
                    let mut waiting = pin!(direction.wait(&group));
                    assert!(waiting.as_mut().poll(&mut cx).is_pending());
                    direction.publish();
                    assert!(waiting.as_mut().poll(&mut cx).is_ready());
                    direction.clear(direction.snapshot());
                }
            }
            (start.elapsed().as_nanos() as u64, 0)
        }
        "dispatch" | "dispatch-waiting" => {
            let records: Vec<_> = (0..width)
                .map(|key| {
                    Box::new(Registration {
                        owner: Weak::new(),
                        key,
                        inactive: AtomicBool::new(false),
                        readable: Direction::new(),
                        writable: Direction::new(),
                    })
                })
                .collect();
            let tokens: Vec<_> = records.iter().map(|record| record.token()).collect();
            let state = DriverState {
                group: group.clone(),
                controller: group.controllers[0].clone(),
                events: epoll::layer_events(&tokens),
            };
            let mut elapsed = 0;
            for _ in 0..iterations {
                let mut waiting: Vec<_> = if kind == "dispatch-waiting" {
                    records
                        .iter()
                        .map(|r| Box::pin(r.readable.wait(&group)))
                        .collect()
                } else {
                    Vec::new()
                };
                for future in &mut waiting {
                    assert!(future.as_mut().poll(&mut cx).is_pending());
                }
                let start = Instant::now();
                state.dispatch().unwrap();
                elapsed += start.elapsed().as_nanos() as u64;
                for future in &mut waiting {
                    assert!(future.as_mut().poll(&mut cx).is_ready());
                }
                for record in &records {
                    assert_ne!(record.readable.snapshot() & 1, 0);
                    record.readable.clear(record.readable.snapshot());
                }
                black_box(&state);
            }
            (elapsed, 0)
        }
        "empty-poll" => {
            let mut worker = drivers.into_iter().next().unwrap().enter();
            // Driver drop must follow the measured loop.
            let start = Instant::now();
            for _ in 0..iterations {
                assert!(!worker.turn(Some(Duration::ZERO)).unwrap());
            }
            return (start.elapsed().as_nanos() as u64, 0);
        }
        "events" => {
            use std::{
                io::{Read, Write},
                os::unix::net::UnixStream,
            };
            let poller = Poller::new().unwrap();
            let mut sockets = Vec::new();
            let mut peers = Vec::new();
            for key in 0..width {
                let (socket, peer) = UnixStream::pair().unwrap();
                socket.set_nonblocking(true).unwrap();
                let socket = socket2::Socket::from(std::os::fd::OwnedFd::from(socket));
                // SAFETY: sockets outlive their poller registrations.
                unsafe {
                    poller
                        .add_with_mode(&socket, Event::readable(key), PollMode::Edge)
                        .unwrap();
                }
                sockets.push(socket);
                peers.push(peer);
            }
            let mut events = Events::new();
            let mut elapsed = 0;
            let mut checksum = 0;
            for _ in 0..iterations {
                for peer in &mut peers {
                    peer.write_all(&[1]).unwrap();
                }
                let start = Instant::now();
                events.clear();
                let count = poller.wait(&mut events, Some(Duration::ZERO)).unwrap();
                assert_eq!(count, width);
                for event in events.iter() {
                    assert!(event.readable);
                    checksum += black_box(event.key);
                }
                elapsed += start.elapsed().as_nanos() as u64;
                for socket in &mut sockets {
                    let mut byte = [0];
                    assert_eq!((&*socket).read(&mut byte).unwrap(), 1);
                }
            }
            for socket in &sockets {
                poller.delete(socket).unwrap();
            }
            (elapsed, checksum)
        }
        "events-driver" => {
            use std::{
                io::{Read, Write},
                os::unix::net::UnixStream,
            };
            let mut worker = drivers.into_iter().next().unwrap().enter();
            let controller = &worker.driver.inner.as_ref().unwrap().controller;
            let records: Vec<_> = (0..width)
                .map(|key| {
                    Box::new(Registration {
                        owner: Weak::new(),
                        key,
                        inactive: AtomicBool::new(false),
                        readable: Direction::new(),
                        writable: Direction::new(),
                    })
                })
                .collect();
            let mut sockets = Vec::new();
            let mut peers = Vec::new();
            for record in &records {
                let (socket, peer) = UnixStream::pair().unwrap();
                socket.set_nonblocking(true).unwrap();
                let socket = socket2::Socket::from(std::os::fd::OwnedFd::from(socket));
                unsafe {
                    controller
                        .poller
                        .add_with_mode(&socket, Event::readable(record.token()), PollMode::Edge)
                        .unwrap();
                }
                sockets.push(socket);
                peers.push(peer);
            }
            let mut elapsed = 0;
            for _ in 0..iterations {
                let mut waiting: Vec<_> = records
                    .iter()
                    .map(|record| Box::pin(record.readable.wait(&group)))
                    .collect();
                for future in &mut waiting {
                    assert!(future.as_mut().poll(&mut cx).is_pending());
                }
                for peer in &mut peers {
                    peer.write_all(&[1]).unwrap();
                }
                let start = Instant::now();
                assert!(!worker.turn(Some(Duration::ZERO)).unwrap());
                elapsed += start.elapsed().as_nanos() as u64;
                for future in &mut waiting {
                    assert!(future.as_mut().poll(&mut cx).is_ready());
                }
                for record in &records {
                    record.readable.clear(record.readable.snapshot());
                }
                for socket in &mut sockets {
                    let mut byte = [0];
                    assert_eq!((&*socket).read(&mut byte).unwrap(), 1);
                }
            }
            let controller = &worker.driver.inner.as_ref().unwrap().controller;
            for socket in &sockets {
                controller.poller.delete(socket).unwrap();
            }
            return (elapsed, 0);
        }
        _ => panic!("unknown Base probe {kind}"),
    };
    drop(drivers);
    result
}
