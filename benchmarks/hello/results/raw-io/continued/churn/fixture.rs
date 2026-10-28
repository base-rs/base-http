
// Diagnostic fixture appended temporarily to selector's private driver tests.
// Real readiness publication overlaps remote registration/deregistration.
#[test]
#[ignore = "isolated performance diagnostic"]
fn routing_churn_diagnostic() {
    use std::sync::atomic::AtomicBool;
    struct Churn {
        start: std::sync::mpsc::Sender<()>,
        done: Mutex<std::sync::mpsc::Receiver<()>>,
        armed: AtomicBool,
    }
    impl Wake for Churn {
        fn wake(self: Arc<Self>) {
            if self.armed.swap(false, Ordering::AcqRel) {
                self.start.send(()).unwrap();
                self.done.lock().unwrap().recv_timeout(Duration::from_secs(3)).unwrap();
            }
        }
    }
    fn usage() -> libc::rusage {
        let mut value = std::mem::MaybeUninit::uninit();
        // SAFETY: getrusage initializes this local output on success.
        assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, value.as_mut_ptr()) }, 0);
        // SAFETY: the call above succeeded.
        unsafe { value.assume_init() }
    }
    fn seconds(value: libc::timeval) -> f64 {
        value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0
    }
    let idle_count: usize = std::env::var("BASE_HTTP_CHURN_IDLE").unwrap().parse().unwrap();
    let rounds: usize = std::env::var("BASE_HTTP_CHURN_ROUNDS").unwrap().parse().unwrap();
    let (selector, mut workers) = setup(1);
    let group = selector.readiness.as_ref().unwrap().clone();
    let idle = (0..idle_count).map(|_| {
        let socket = packet();
        socket.registrations.get(&group, socket.get_ref()).unwrap();
        socket
    }).collect::<Vec<_>>();
    let hot = packet();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let (start, requests) = std::sync::mpsc::channel();
    let (done, completions) = std::sync::mpsc::channel();
    let wake = Arc::new(Churn { start, done: Mutex::new(completions), armed: AtomicBool::new(false) });
    let remote = std::thread::spawn(move || {
        for () in requests {
            for _ in 0..8 {
                let socket = packet();
                socket.registrations.get(&group, socket.get_ref()).unwrap();
                drop(socket);
            }
            done.send(()).unwrap();
        }
    });
    let waker = Waker::from(wake.clone());
    let mut before = usage();
    let mut started = std::time::Instant::now();
    for round in 0..rounds + 20 {
        if round == 20 { before = usage(); started = std::time::Instant::now(); }
        let mut reading = Box::pin(read(&selector, &hot));
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(reading.as_mut().poll(&mut cx).is_pending());
        wake.armed.store(true, Ordering::Release);
        sender.send_to(b"x", hot.local_addr().unwrap()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if let Poll::Ready((result, bytes)) = reading.as_mut().poll(&mut cx) {
                assert_eq!(result.unwrap(), 1);
                assert_eq!(bytes, b"x");
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            workers[0].turn(Some(Duration::from_millis(10))).unwrap();
        }
        assert!(!wake.armed.load(Ordering::Acquire));
    }
    let elapsed = started.elapsed().as_secs_f64();
    let after = usage();
    println!("{{\"idle\":{idle_count},\"rounds\":{rounds},\"seconds\":{elapsed},\"user_seconds\":{},\"system_seconds\":{}}}",
        seconds(after.ru_utime) - seconds(before.ru_utime), seconds(after.ru_stime) - seconds(before.ru_stime));
    drop(waker);
    drop(wake);
    remote.join().unwrap();
    drop(idle);
    drop(hot);
    block_on(selector.shutdown());
}
