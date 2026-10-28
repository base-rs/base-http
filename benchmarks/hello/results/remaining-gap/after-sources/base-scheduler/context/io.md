# Writing efficient I/O

Use scheduler-bound readers and writers for repeated I/O. Construct them once from the selected scheduler and resource, retain them across operations, and pass cancellation to each call. This keeps reusable backend state on the ordinary application path without choosing polling, registered buffers, multishot, or worker routing.

## Choose by the resource and data lifetime

| Application requirement | Starting point |
| --- | --- |
| Read a TCP or other byte stream | `scheduler.socket_stream_reader(&socket)`; use `read_into` for reused parser storage or `read` for independently owned chunks. |
| Write a byte stream | `scheduler.socket_stream_writer(&socket)`; use `write` or `write_vectored` for borrowed output, and `submit` when accepted owned output should progress while the application prepares more work. |
| Receive UDP or other packets | `scheduler.socket_packet_reader(&socket)`; use `read_into` or `read_vectored` inside a receive loop, or `read` to hand complete owned messages to independent tasks. |
| Send packets | `scheduler.socket_packet_writer(&socket)`; use `write` or `write_vectored` for borrowed storage, or `write_owned` to transfer a stable owner through completion. |
| Copy or transform files | `scheduler.file_reader(&input)` and `scheduler.file_writer(&output)`, reusable owned buffers, and joined independent reads/writes. |

Each handle represents one sequential operation stream. Create independent readers and writers for concurrent execution; several file readers or writers may share the same underlying file. Keep the scheduler and its drivers alive while those handles operate and close. A task moving to another worker does not change its handle's selected scheduler; the backend handles worker-local registration and migration internally.

## Own chunks only when useful

A stream reader's `read(cancellation)` returns an owned chunk or `None` at EOF. A packet reader's `read(cancellation)` returns an owned message or `None` after local close; an empty datagram is a real message. Completed storage may be parsed, frozen, retained, or moved to another task while later receives continue. `address()` and `payload()` borrow the message's metadata and completed bytes. Preserve the peer endpoint before extracting payload ownership if it is needed afterwards, and inspect packet truncation flags.

For parser-owned scratch storage, reuse `read_into(&mut bytes, cancellation)` or `read_vectored(&mut slices, cancellation)` on the same reader. Borrowed bytes are accessed only synchronously inside a poll; a pending operation leaves no independent kernel or worker access to them. Storage may be inline or a reused `Vec`; choose the layout the parser needs. Stack placement alone is not the optimization.

Readers create default pool storage lazily for owned reception. Borrowed-only use does not allocate a receive pool. To share a specific budget across connections, create it with `scheduler.buffer_pool(options)` and pass `pool.clone()` to `socket_stream_reader_with_pool` or `socket_packet_reader_with_pool`. Pooling supplies ownership, retention, reuse, and backpressure; it does not inherently make a receive faster. The backend retains control of allocation, registration, publication, and hot-buffer reuse.

Parse while a chunk is uniquely writable when mutation is useful. Freeze it before retaining immutable views. Completed frames wholly inside one chunk can retain ranges; copy or assemble only the incomplete frame crossing chunks when contiguous records are required. A small long-lived view can pin an entire slot, so selective copying can improve memory use. Framing, decryption, and compaction belong to the protocol.

Budget for application-held views and in-flight operations. Bound packet tasks and retained frames; holding every slot while awaiting another read prevents progress. `BufferPoolOptions::buffers` is the exact shared count, independent of worker count, and exhaustion backpressures rather than growing. Do not depend on worker-local allocation or a particular recycling order.

## Preserve output ordering

A stream writer's `write` and `write_vectored` make one possibly partial transfer. Advance the borrowed slices after a short result. Keep one writer for each ordered output sequence; separate writers and socket aliases do not share ordering.

Use `submit(owner, cancellation)` when retaining owned output lets the application prepare or receive the next chunk while the accepted write progresses. Submission finishes its predecessor before accepting the new owner. Rejection returns the new owner unchanged. Accepted partial-write retries progress independently of further writer calls, and completed owners are released without requiring `flush` to collect them. `flush(cancellation)` awaits acceptance of every submitted byte by the socket; it does not establish peer acknowledgement. Its cancellation ends only that flush wait.

Overlap input and output explicitly. The first read produces a chunk; submit its owned response, then await the next read while that output progresses. The next submission waits for the preceding output before accepting another owner. Two pool slots accommodate one read and one write; retained parser views require additional capacity. Ordinary readers receive on demand and keep no hidden receive-ahead queue. For packet operations, poll independent reads and writes together when overlap is useful.

For packets, each write preserves one message boundary and accepts an optional destination address; `None` uses a connected peer. `write_owned` returns the owner alongside the completed result. Retain the complete received message when handing it to a task that also needs its source metadata.

## Keep file position explicit

`FileReader::read` and `FileWriter::write` use the underlying OS file cursor. Handles do not invent private logical positions. Multiple handles or aliases share that cursor, so serialize dependent cursor operations. Use `read_at(buffer, offset, cancellation)` and `write_at(buffer, offset, cancellation)` for explicit positions, subject to the documented platform cursor behavior. `write_all` sequences short writes and returns the original owner even on error or cancellation; success is not a durability guarantee.

For a file pipeline, poll independent reads and writes together with `zip`/join. Merely constructing both futures submits neither. Two reusable buffers suffice for a one-read/one-write pipeline. If using pooled buffers, release a completed write's owner inside its branch so a capacity-waiting read can progress. `FileReader` and `FileWriter` also offer separate submission/completion methods with compatible fallbacks. Use them when the acceptance boundary itself matters; ordinary overlap needs only the joined operations.

## Finish cleanup

Cancellation requests progress toward shutdown, not rollback. A successful transfer can win a cancellation race. Capture a loop's result, await the reader or writer's uncancellable `close()`, drop the handle, then return the result. Flush stream output before close on success; close still runs after an operation error. Only successful close proves backend retirement. Once close begins, perform only close retries through that handle.

Close and drop every reader and writer before exclusively closing its socket or file. Await all accepted task handles, including on failure; dropping a task handle requests task destruction. Applications decide how to report an operation failure alongside a cleanup failure. The examples prioritize cleanup failures.

## Keep examples and measurements aligned

Use the [canonical I/O recipes](https://github.com/base-rs/base-scheduler/tree/main/examples) as starting points. Extend those workloads when changing a recommendation rather than creating an unrelated favorable microbenchmark.

Use optimized builds, execute I/O on scheduler workers, validate received contents, and record setup, warmup, timing boundaries, payload sizes, pool budgets, worker counts, environment, and source revision. Compare equivalent ownership and parser behavior: immediate borrowed consumption and retaining frames solve different problems. Monitors, receivers, operation dispatch, and selector registration are backend implementation tools. Keep them out of ordinary protocol call sites, and measure multishot, registered buffers, and zero-copy mechanisms behind the same reader/writer interface.

Adapters implement the same public reader/writer contracts and keep dispatch, registration, receive strategy, and retirement state private. Protocol libraries request only the resource capabilities they need. Separate file admission/completion methods are available on `FileReader` and `FileWriter`; ordinary joined operations remain the default pipeline.
