//! Temporary, single-worker probes. Never linked into capacity/latency binaries.
use std::{
    cell::RefCell,
    collections::HashMap,
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
    time::Instant,
};

#[derive(Clone, Copy)]
pub struct Publication {
    at: Instant,
    collection: u64,
    batch_len: usize,
}
struct Pending {
    publication: Publication,
    stages: [Instant; 5], // CQ end, driver return, task poll, inner poll, consumption.
    ready: Option<Instant>,
    inner_index: u64,
}
struct State {
    random: u64,
    collection: u64,
    batch_len: usize,
    ends: [Option<(u64, Instant)>; 256],
    driver_return: Option<Instant>,
    task_start: Option<Instant>,
    inner_start: Option<Instant>,
    inner_index: u64,
    pending: HashMap<u64, Pending>,
    next_connection: u64,
    connection: Option<u64>,
    rows: Vec<[u64; 10]>,
}
impl Default for State {
    fn default() -> Self {
        Self {
            random: 0x4d595df4d0f33173,
            collection: 0,
            batch_len: 0,
            ends: [None; 256],
            driver_return: None,
            task_start: None,
            inner_start: None,
            inner_index: 0,
            pending: HashMap::new(),
            next_connection: 0,
            connection: None,
            rows: Vec::new(),
        }
    }
}
thread_local! { static STATE: RefCell<State> = RefCell::new(State::default()); }

pub fn collection_begin(len: usize) {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        s.collection += 1;
        s.batch_len = len;
    });
}
pub fn collection_end() {
    let now = Instant::now();
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let id = s.collection;
        s.ends[id as usize % 256] = Some((id, now));
    });
}
pub fn driver_return() {
    let now = Instant::now();
    STATE.with(|state| state.borrow_mut().driver_return = Some(now));
}
pub fn publication() -> Option<Publication> {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        s.random = s
            .random
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        if s.random >> 57 != 0 {
            return None;
        }
        Some(Publication {
            at: Instant::now(),
            collection: s.collection,
            batch_len: s.batch_len,
        })
    })
}
pub fn consumed(publication: Option<Publication>) {
    let Some(publication) = publication else {
        return;
    };
    let now = Instant::now();
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let (id, cq_end) = s.ends[publication.collection as usize % 256].unwrap();
        assert_eq!(id, publication.collection, "sample outlived CQ history");
        let stages = [
            cq_end,
            s.driver_return.unwrap(),
            s.task_start.unwrap(),
            s.inner_start.unwrap(),
            now,
        ];
        let mut previous = publication.at;
        for stage in stages {
            assert!(stage >= previous, "probe stage order");
            previous = stage;
        }
        let identifier = s.connection.unwrap();
        let inner_index = s.inner_index;
        assert!(
            s.pending
                .insert(
                    identifier,
                    Pending {
                        publication,
                        stages,
                        ready: None,
                        inner_index,
                    }
                )
                .is_none(),
            "sample was not paired with its response"
        );
    });
}
pub fn read_ready() {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let identifier = s.connection.unwrap();
        if let Some(pending) = s.pending.get_mut(&identifier) {
            pending.ready = Some(Instant::now());
        }
    });
}
pub fn response_written() {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        let identifier = s.connection.unwrap();
        let Some(pending) = s.pending.remove(&identifier) else {
            return;
        };
        let now = Instant::now();
        let times = [
            pending.publication.at,
            pending.stages[0],
            pending.stages[1],
            pending.stages[2],
            pending.stages[3],
            pending.stages[4],
            pending.ready.unwrap(),
            now,
        ];
        let mut row = [0; 10];
        for i in 0..7 {
            row[i] = times[i + 1]
                .duration_since(times[i])
                .as_nanos()
                .try_into()
                .unwrap();
        }
        row[7] = now
            .duration_since(pending.publication.at)
            .as_nanos()
            .try_into()
            .unwrap();
        row[8] = pending.inner_index;
        row[9] = pending.publication.batch_len as u64;
        assert_eq!(row[..7].iter().sum::<u64>(), row[7]);
        s.rows.push(row);
        if s.rows.len() == 2048 {
            eprintln!("TRACE {:?}", s.rows);
            s.rows.clear();
        }
    });
}
pub fn flush() {
    STATE.with(|state| {
        let mut s = state.borrow_mut();
        if !s.rows.is_empty() {
            eprintln!("TRACE {:?}", s.rows);
            s.rows.clear();
        }
    });
}
pub async fn task<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    poll_fn(|cx| {
        STATE.with(|state| {
            let mut s = state.borrow_mut();
            s.task_start = Some(Instant::now());
            s.inner_index = 0;
        });
        future.as_mut().poll(cx)
    })
    .await
}
pub async fn connection<F: Future>(future: F) -> F::Output {
    let identifier = STATE.with(|state| {
        let mut s = state.borrow_mut();
        s.next_connection += 1;
        s.next_connection
    });
    let mut future = pin!(future);
    poll_fn(|cx| {
        STATE.with(|state| {
            let mut s = state.borrow_mut();
            s.connection = Some(identifier);
            s.inner_start = Some(Instant::now());
            s.inner_index += 1;
        });
        let result = future.as_mut().poll(cx);
        if matches!(result, Poll::Ready(_)) {
            flush();
        }
        result
    })
    .await
}
