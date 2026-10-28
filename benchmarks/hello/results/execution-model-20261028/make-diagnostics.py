"""Separate diagnostic sources; these are never production timing binaries."""
from pathlib import Path
root=Path(__file__).resolve().parent
instrumentation=r'''
#[derive(Default)]
struct Diagnostic {
    turns: u64,
    payload_batches: u64,
    payload_requests: u64,
    deferred_batches: u64,
    deferred_requests: u64,
    submits: u64,
    waits: u64,
    deferred_at: Option<std::time::Instant>,
    delays: u64,
    delay_ns: u128,
    max_delay_ns: u128,
    delay_buckets: [u64; 13],
    admissions: u64,
    queue_delays: u64,
    queue_delay_ns: u128,
    max_queue_delay_ns: u128,
    queue_buckets: [u64; 13],
    publications: u64,
    poll_delays: u64,
    poll_delay_ns: u128,
    max_poll_delay_ns: u128,
    poll_buckets: [u64; 13],
}
std::thread_local! {
    static DIAGNOSTIC: std::cell::RefCell<Diagnostic> = std::cell::RefCell::new(Diagnostic::default());
}
fn diagnostic(action: &str, payloads: u64) {
    DIAGNOSTIC.with(|slot| {
        let mut d = slot.borrow_mut();
        match action {
            "turn" => {
                d.turns += 1;
                if d.turns % 4096 == 0 {
                    eprintln!("DIAG {{\"turns\":{},\"payload_batches\":{},\"payload_requests\":{},\"deferred_batches\":{},\"deferred_requests\":{},\"submits\":{},\"waits\":{},\"delays\":{},\"delay_sum_ns\":{},\"max_delay_ns\":{},\"delay_buckets_le_us\":{:?}}}", d.turns,d.payload_batches,d.payload_requests,d.deferred_batches,d.deferred_requests,d.submits,d.waits,d.delays,d.delay_ns,d.max_delay_ns,d.delay_buckets);
                    eprintln!("POLL {{\"publications\":{},\"samples\":{},\"sum_ns\":{},\"max_ns\":{},\"buckets_le_us\":{:?}}}",d.publications,d.poll_delays,d.poll_delay_ns,d.max_poll_delay_ns,d.poll_buckets);
                    eprintln!("QUEUE {{\"admissions\":{},\"samples\":{},\"sum_ns\":{},\"max_ns\":{},\"buckets_le_us\":{:?}}}",d.admissions,d.queue_delays,d.queue_delay_ns,d.max_queue_delay_ns,d.queue_buckets);
                }
            }
            "prepare" => { if payloads > 0 {d.payload_batches+=1; d.payload_requests+=payloads;} }
            "defer" => {
                if payloads > 0 {
                    assert!(d.deferred_at.is_none());
                    d.deferred_batches+=1; d.deferred_requests+=payloads;
                    d.deferred_at=Some(std::time::Instant::now());
                }
            }
            "submit" | "wait" => {
                if action == "submit" {d.submits+=1;} else {d.waits+=1;}
                if let Some(start)=d.deferred_at.take() {
                    let ns=start.elapsed().as_nanos();
                    d.delays+=1; d.delay_ns+=ns; d.max_delay_ns=d.max_delay_ns.max(ns);
                    let limits=[1,2,4,8,16,32,64,128,256,512,1024,2048];
                    let index=limits.iter().position(|us| ns <= us*1000).unwrap_or(12);
                    d.delay_buckets[index]+=1;
                }
            }
            _ => unreachable!(),
        }
    });
}
fn diagnostic_admit_sample() -> bool {
    DIAGNOSTIC.with(|slot| {
        let mut d=slot.borrow_mut();
        d.admissions+=1;
        d.admissions%128==0
    })
}
fn diagnostic_queue_delay(start: std::time::Instant) {
    let ns=start.elapsed().as_nanos();
    DIAGNOSTIC.with(|slot| {
        let mut d=slot.borrow_mut();
        d.queue_delays+=1; d.queue_delay_ns+=ns; d.max_queue_delay_ns=d.max_queue_delay_ns.max(ns);
        let limits=[1,2,4,8,16,32,64,128,256,512,1024,2048];
        let index=limits.iter().position(|us| ns <= us*1000).unwrap_or(12);
        d.queue_buckets[index]+=1;
    });
}
fn diagnostic_sample() -> bool {
    DIAGNOSTIC.with(|slot| {
        let mut d=slot.borrow_mut();
        d.publications+=1;
        d.publications%128==0
    })
}
fn diagnostic_poll_delay(start: std::time::Instant) {
    let ns=start.elapsed().as_nanos();
    DIAGNOSTIC.with(|slot| {
        let mut d=slot.borrow_mut();
        d.poll_delays+=1; d.poll_delay_ns+=ns; d.max_poll_delay_ns=d.max_poll_delay_ns.max(ns);
        let limits=[1,2,4,8,16,32,64,128,256,512,1024,2048];
        let index=limits.iter().position(|us| ns <= us*1000).unwrap_or(12);
        d.poll_buckets[index]+=1;
    });
}
'''
for variant in ('baseline','candidate'):
    source=(root/f'{variant}.rs').read_text()
    source=source.replace('const NOTIFICATION_TAG: u64 = u64::MAX;', 'const NOTIFICATION_TAG: u64 = u64::MAX;\n'+instrumentation)
    source=source.replace('self.started = true;\n        let (mut progressed', 'self.started = true;\n        diagnostic("turn",0);\n        let (mut progressed')
    source=source.replace('let defer_submission;\n', 'let defer_submission;\n        let mut payloads = 0;\n')
    source=source.replace('let entry = request.entry();\n                progressed', 'let entry = request.entry();\n                payloads += u64::from(entry.get_opcode() != u32::from(opcode::PollAdd::CODE));\n                progressed')
    source=source.replace('''        if self.closing
            && !command_batch_full''','''        diagnostic("prepare", payloads);
        if defer_submission {diagnostic("defer",payloads);}
        if self.closing
            && !command_batch_full''')
    source=source.replace('let result = syscall("submit", || self.ring.submit());', 'diagnostic("submit",0);\n                let result = syscall("submit", || self.ring.submit());')
    source=source.replace('let result = syscall("wait", || match timeout {', 'diagnostic("wait",0);\n        let result = syscall("wait", || match timeout {')
    assert source.count('diagnostic("turn",0)')==1
    assert source.count('payloads +=')==1
    assert source.count('diagnostic("prepare", payloads)')==1
    (root/f'diagnostic-{variant}.rs').write_text(source)

# Resolve from the script location, independently of the caller's cwd.
source=root.parents[4]/'base-scheduler-execution-model/crates/iouring/src/io_uring_selector/socket_read.rs'
source=source.read_text()
source=source.replace('collecting: false,','collecting: false,\n                    diagnostic_published: None,',1)
source=source.replace('    collecting: bool,','    collecting: bool,\n    diagnostic_published: Option<std::time::Instant>,',1)
source=source.replace('                slot.result = Some(result);','                slot.diagnostic_published=super::diagnostic_sample().then(std::time::Instant::now);\n                slot.result = Some(result);',1)
source=source.replace('''        let result = {
            let mut slot = self.shared.slot.lock().unwrap();
            slot.collecting = false;
            slot.result.take().unwrap()
        };''','''        let (result, published) = {
            let mut slot = self.shared.slot.lock().unwrap();
            slot.collecting = false;
            (slot.result.take().unwrap(), slot.diagnostic_published.take())
        };
        if let Some(start)=published {super::diagnostic_poll_delay(start);}''')
source=source.replace('    pointer: *mut u8,','    pointer: *mut u8,\n    diagnostic_queued: Option<std::time::Instant>,',1)
source=source.replace('                    pointer: std::ptr::null_mut(),','                    pointer: std::ptr::null_mut(),\n                    diagnostic_queued: None,',1)
source=source.replace('    fn entry(&mut self) -> squeue::Entry {','    fn entry(&mut self) -> squeue::Entry {\n        if let Some(start)=self.diagnostic_queued.take() {super::diagnostic_queue_delay(start);}',1)
source=source.replace('        completion.cancel.controller.submit(request);','        request.diagnostic_queued=super::diagnostic_admit_sample().then(std::time::Instant::now);\n        completion.cancel.controller.submit(request);',1)
assert source.count('diagnostic_queue_delay(start)')==1
assert source.count('diagnostic_poll_delay(start)')==1
(root/'diagnostic-socket-read.rs').write_text(source)
