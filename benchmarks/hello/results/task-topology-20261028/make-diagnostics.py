"""Build separate probe sources from the frozen production sources; no runtime fix."""
from pathlib import Path
import argparse

p=argparse.ArgumentParser();p.add_argument('root',type=Path);a=p.parse_args();root=a.root.resolve()
def change(path, old, new, count=1):
    source=path.read_text(); assert source.count(old)==count,(path,old,source.count(old));path.write_text(source.replace(old,new))
scheduler=root/'scheduler'; hello=root/'http/base-http/benchmarks/hello'
(scheduler/'crates/iouring/src/diagnostic.rs').write_text((root/'diagnostic.rs').read_text())
change(scheduler/'crates/iouring/src/lib.rs','mod fallback;','pub mod diagnostic;\nmod fallback;')
change(scheduler/'crates/iouring/src/io_uring_worker.rs','        self.driver.turn(timeout)','        let result=self.driver.turn(timeout);\n        crate::diagnostic::driver_return();\n        result')
selector=scheduler/'crates/iouring/src/io_uring_selector.rs'
change(selector,'        for completion in &mut completions {','        crate::diagnostic::collection_begin(completions.len());\n        for completion in &mut completions {')
change(selector,'        drop(completions);','        drop(completions);\n        crate::diagnostic::collection_end();')
read=scheduler/'crates/iouring/src/io_uring_selector/socket_read.rs'
change(read,'                    collecting: false,','                    collecting: false,\n                    diagnostic_published: None,')
change(read,'    collecting: bool,','    collecting: bool,\n    diagnostic_published: Option<crate::diagnostic::Publication>,')
change(read,'                slot.result = Some(result);','''                slot.diagnostic_published=if matches!(&result,Completion::Request(Ok(count),_) if *count>0) {crate::diagnostic::publication()} else {None};
                slot.result = Some(result);''')
change(read,'''        let result = {
            let mut slot = self.shared.slot.lock().unwrap();
            slot.collecting = false;
            slot.result.take().unwrap()
        };''','''        let (result,published) = {
            let mut slot = self.shared.slot.lock().unwrap();
            slot.collecting = false;
            (slot.result.take().unwrap(),slot.diagnostic_published.take())
        };
        crate::diagnostic::consumed(published);''')
raw=hello/'src/raw.rs'
change(raw,'                check_write(writer.write(RESPONSE, cancellation).await?)?;','''                base_scheduler_iouring::diagnostic::read_ready();
                check_write(writer.write(RESPONSE, cancellation).await?)?;
                base_scheduler_iouring::diagnostic::response_written();''')
change(raw,'connections.push(base_connection::<_, OWNED_RECEIVE>(', 'connections.push(base_scheduler_iouring::diagnostic::connection(base_connection::<_, OWNED_RECEIVE>(')
change(raw,'''            &cancellation,
        ));''','''            &cancellation,
        )));''')
change(raw,'        base_connection::<_, OWNED_RECEIVE>(shared.as_ref(), socket, &cancellation).await','        base_scheduler_iouring::diagnostic::task(base_scheduler_iouring::diagnostic::connection(base_connection::<_, OWNED_RECEIVE>(shared.as_ref(), socket, &cancellation))).await')
main=hello/'src/main.rs'
change(main,'if owned_receive { raw::base_server::<_, true>(shared.as_ref()).await }','if owned_receive { base_scheduler_iouring::diagnostic::task(raw::base_server::<_, true>(shared.as_ref())).await }')
