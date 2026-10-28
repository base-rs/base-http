# Scheduler interface reference

The application I/O interface consists of scheduler-bound stream and packet readers and writers, plus file readers and writers. Construct them from the chosen scheduler, then pass cancellation to each operation. Reusable libraries can accept these resource traits directly. [I/O usage guidance](io.md) describes which form fits the data lifetime; [design](design.md) records backend contracts and implementation decisions.

## Reading the signatures

Tables omit `&self` or `&mut self` and show each asynchronous method's awaited result. `C` means `&Cancellation`; `B` is a stable owned buffer; `F` and `K` are references to the constructing scheduler's associated file and socket types. Factories are synchronous and return `io::Result<Handle>`.

- `OperationResult<T>` is `Result<T, OperationError>`, where `OperationError` is `Cancelled(Cancelled)` or `Io(io::Error)`. Its `kind()` maps cancellation to `Interrupted`; match the variant to distinguish cancellation from OS interruption. Native I/O codes are preserved when present.
- `BufferResult<B>` is `(OperationResult<usize>, B)`. The original owner is returned on every normally completed result, including errors and cancellation. It is outside `Result` deliberately.
- `close()` has no cancellation argument and returns `io::Result<()>`. Only successful close establishes backend retirement. Close and drop all handles before exclusively closing their underlying resource.
- Operation futures are lazy unless explicitly documented otherwise. Synchronous factories, task spawning, and signal subscription are explicit exceptions. Pre-cancellation prevents cancellable submission; successful progress can win concurrent cancellation. Cancellation is not rollback.
- Owned operations retain buffers and resources until backend access retires, even when a caller drops or forgets its future. Abandonment forfeits recovery of the owner; it cannot free memory still in use.
- Borrowed socket operations access buffers only synchronously inside a poll. A pending future leaves no independent kernel or worker access to those bytes or I/O vectors.
- Operation futures and I/O handles are `Send`. Static dispatch and implementation-associated types do not require a boxed future or runtime-selection features. Spawned work and its owned captures are `Send + 'static`.
- A handle retains its creating scheduler and compatible resource. Its operation methods accept no replacement scheduler. Moving between workers leaves routing and registration migration internal; the scheduler and its drivers must remain alive while operations and cleanup run.

## Bound socket I/O

Use one handle per sequential execution, with independent readers and writers for concurrent input and output. Factories retain resource ownership and reusable backend state; they perform no payload I/O. Stream factories require stream sockets, and packet factories reject stream sockets and support packet-oriented transports such as datagrams.

| Factory capability | Constructor | Resource trait |
| --- | --- | --- |
| [`SocketStreamReading`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_stream_reading.rs) | `socket_stream_reader(K)` | `SocketStreamReader` |
| [`SocketStreamWriting`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_stream_writing.rs) | `socket_stream_writer(K)` | `SocketStreamWriter<Buffer = Pool::Buffer>` |
| [`SocketPacketReading`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_packet_reading.rs) | `socket_packet_reader(K)` | `SocketPacketReader` |
| [`SocketPacketWriting`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_packet_writing.rs) | `socket_packet_writer(K)` | `SocketPacketWriter` |

The default stream writer accepts the same pooled owner type returned by the default stream reader, with no type annotation needed for borrowed-only output. `socket_stream_writer_with_buffer::<B>(K)` selects another stable owner type. Reader factories allocate no payload pool; the first owned read initializes default storage lazily. `socket_stream_reader_with_pool(K, pool)` and `socket_packet_reader_with_pool(K, pool)` select an application-owned shared budget at construction. Factories return implementation-selected handles through the reader/writer contracts; backends choose their composition without changing application operations.

The Direct and Tokio backends provide all four socket handles. Native forwards its selector’s capabilities; both readiness and io\_uring provide all four.

### Stream input

[`SocketStreamReader`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_stream_reader.rs) associates `Buffer: ReceiveBuffer`.

| Method | Arguments → awaited result | Semantics |
| --- | --- | --- |
| `read` | `(C) → OperationResult<Option<Buffer>>` | One owned initialized chunk. `None` means EOF or closed input. The returned chunk can outlive the reader and subsequent reads. |
| `read_into` | `(&mut [u8], C) → OperationResult<usize>` | One possibly short borrowed transfer. Zero with nonempty capacity means EOF; an empty destination completes immediately after handle and cancellation checks, without establishing EOF. |
| `read_vectored` | `(&mut [IoSliceMut], C) → OperationResult<usize>` | One scatter read into borrowed initialized ranges. No read-exact or framing behavior. |
| `close` | `() → io::Result<()>` | Stop input and await registration retirement. Does not close or shut down the socket; returned chunks remain valid. |

Ordinary errors and cancellation end the current wait without closing the reader or rolling back consumed bytes. An owned read waiting for storage is cancellable. Idle demand-driven reception returns its payload storage before waiting for readiness, so unused connections need not reserve the entire shared budget.

### Stream output

[`SocketStreamWriter`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_stream_writer.rs) associates `Buffer: IoBuf`.

| Method | Arguments → awaited result | Semantics |
| --- | --- | --- |
| `write` | `(&[u8], C) → OperationResult<usize>` | Await prior submitted output, then make one possibly partial borrowed transfer. The unwritten suffix remains the caller's responsibility. |
| `write_vectored` | `(&[IoSlice], C) → OperationResult<usize>` | The same ordering with borrowed vectors; short progress does not advance the caller's slices. |
| `submit` | `(Buffer, C) → Result<(), (OperationError, Buffer)>` | Finish the prior logical write, then accept the complete new owner. Rejection returns it unchanged and writes none of it. Accepted partial-write retries progress without further handle polling. |
| `flush` | `(C) → OperationResult<()>` | Await all accepted output or its terminal error. Cancelling this wait does not cancel accepted output. Success means socket acceptance, not peer acknowledgement or a future zero-copy memory-retirement notification. |
| `close` | `() → io::Result<()>` | Stop admission, cancel unfinished output, and await backend access and registration retirement. No implicit flush or socket shutdown. |

The submission token continues to govern accepted output until its send result. An accepted cancellation or error ends that output sequence because a transferred prefix cannot be rolled back. Owners are released as soon as backend access safely ends, independently of flush. After close begins, perform only close retries. Separate writers and direct socket aliases do not share ordering.

### Packet input and output

[`SocketPacketReader`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_packet_reader.rs) associates an owned `Message: SocketRecvMessageMut`. Each successful read represents one packet.

| Method | Arguments → awaited result | Semantics |
| --- | --- | --- |
| `read` | `(C) → OperationResult<Option<Message>>` | One independently owned message with payload, source metadata, and truncation status. An empty datagram is `Some`; `None` means closed input. |
| `read_into` | `(&mut [u8], C) → OperationResult<SocketRecvResult>` | Receive one packet into borrowed storage, preserving source and truncation metadata. |
| `read_vectored` | `(&mut [IoSliceMut], C) → OperationResult<SocketRecvResult>` | Receive one packet across borrowed ranges; preserve the same message metadata. |
| `close` | `() → io::Result<()>` | Stop input and await registration retirement, preserving already returned messages. |

[`SocketPacketWriter`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_packet_writer.rs) sends one packet per operation. `address: Option<SocketAddress>` selects a destination; `None` uses the connected peer.

| Method | Arguments → awaited result | Semantics |
| --- | --- | --- |
| `write` | `(&[u8], address, C) → OperationResult<usize>` | Send one packet from borrowed storage. |
| `write_vectored` | `(&[IoSlice], address, C) → OperationResult<usize>` | Gather borrowed ranges into one packet without requiring a contiguous application copy. |
| `write_owned` | `(B: IoBuf, address, C) → BufferResult<B>` | Retain a stable owner through one send and return it alongside the result. |
| `close` | `() → io::Result<()>` | Await registration retirement without closing or shutting down the socket. |

`SocketRecvResult` contains the copied byte count, optional source address, payload truncation, and ancillary-data truncation. Ancillary bytes themselves are not exposed. Ordinary packet reads consume packets; explicit receive flags can request peeking. Buffer registration remains an implementation strategy.

## Bound file I/O

[`FileReading::file_reader`](https://github.com/base-rs/base-scheduler/blob/main/src/file_reading.rs) and [`FileWriting::file_writer`](https://github.com/base-rs/base-scheduler/blob/main/src/file_writing.rs) synchronously construct scheduler-bound handles from `F`. Reuse each handle for one sequential operation stream. Independent handles may operate on the same file concurrently.

| Resource/method | Arguments → awaited result | Semantics |
| --- | --- | --- |
| `FileReader::read` | `(B: IoBufMut, C) → BufferResult<B>` | One initialized-buffer read at the OS cursor; preserve short counts. |
| `FileReader::read_at` | `(B: IoBufMut, offset: u64, C) → BufferResult<B>` | One read at an explicit position. |
| `FileWriter::write` | `(B: IoBuf, C) → BufferResult<B>` | One potentially partial write at the OS cursor. |
| `FileWriter::write_at` | `(B: IoBuf, offset: u64, C) → BufferResult<B>` | One write at an explicit position. |
| `FileWriter::write_all` | `(B: IoBuf, C) → BufferResult<B>` | Sequence short writes; success reports the full length, and zero progress on nonempty output is `WriteZero`. The original owner is returned even after partial effects. |
| Either handle's `close` | `() → io::Result<()>` | Stop admission and await accepted operation and registration retirement; drop the handle afterwards. Does not close the file. |

Handles have no private logical position. Ordinary reads and writes share the underlying OS cursor with every file alias; serialize dependent cursor operations. Offsets must fit in `i64`. Unix positioned operations leave the shared cursor unchanged; Windows implementations may use `seek_read` and `seek_write`, which change it. Handles do not emulate a private cursor. Success does not establish durability: finish writes before `file_sync`.

Owned file operations may outlive dropped futures. Reusing that sequential handle can return `WouldBlock` while accepted access or an uncollected completion reserves its reusable state. Joining independent reader and writer futures overlaps work without an extra task or a submission-specific API.

### Separate file submission waits

`FileReader` exposes `read_submit` and `read_at_submit`; `FileWriter` exposes `write_submit` and `write_at_submit`. Arguments match the ordinary methods, with per-call cancellation and no scheduler argument. Awaiting admission returns:

```rust,ignore
Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)>
```

Rejection returns the unchanged input owner without transferring bytes. After admission, the returned completion owns its operation state independently of the handle and cancellation reference. Accepted work can progress without polling it; a portable fallback may complete the ordinary operation before returning admission. Await completion to recover the owner and operation result. Dropping the completion forfeits recovery but never frees storage still in use. Another operation on that sequential handle can report `WouldBlock` while the operation or an uncollected completion reserves reusable state; `close()` waits for that retirement, even if the completion remains uncollected. Admission does not establish durability. Joining ordinary independent reader/writer operations remains the simple pipeline choice.

## Explicit socket transfer options

The ordinary methods remain the fast path. Use these variants when protocol semantics require flags or ownership of a complete scatter/gather sequence:

| Method | Arguments → awaited result | Semantics |
| --- | --- | --- |
| Reader `read_into_with_flags` | `(&mut [u8], SocketRecvFlags, C) → usize` for streams, `SocketRecvResult` for packets | One borrowed receive; `peek` observes bytes without consuming them. |
| Reader `read_vectored_with_flags` | `(&mut [IoSliceMut], SocketRecvFlags, C) → SocketRecvResult` | Borrowed scatter receive retaining metadata and truncation flags. |
| Reader `read_owned` | `(B: IoBufMut, C) → (OperationResult<count>, B)` | Stable owner returned on every result; packet count includes receive metadata. |
| Reader `read_owned_vectored` | `(Buffers: IoBufSequenceMut, SocketRecvFlags, C) → (OperationResult<SocketRecvResult>, Buffers)` | One owned scatter receive. The owner and its ranges are returned unchanged. |
| Writer `write_with_flags` / `write_vectored_with_flags` | `(borrowed bytes/vectors, [destination], SocketSendFlags, C) → usize` | One borrowed transfer; `dont_route` requests local routing. Packets accept an optional destination. |
| Writer `write_owned` | `(B: IoBuf, [destination], C) → BufferResult<B>` | One owned transfer returning the owner on every result. |
| Writer `write_owned_vectored` | `(Buffers: IoBufSequence, [destination], SocketSendFlags, C) → (OperationResult<usize>, Buffers)` | One owned gather send; packets preserve one message boundary. |

Destination arguments apply to packet writers only. Stream writes await earlier submitted output before making another transfer. No vectored method advances the caller's vectors. An owned sequence retains every buffer and stable native I/O vector until access retires, including after the waiter is dropped or forgotten. Borrowed variants access bytes only synchronously within each poll. These contracts do not prescribe copies, allocations, or a particular backend mechanism. Abandoning an owned operation can leave its sequential lane busy, with successors reporting `WouldBlock` until backend access retires. Await `close()` to establish retirement.

## Filesystem control

| Capability/hook | Arguments → awaited result | Semantics |
| --- | --- | --- |
| [`FileOpen::file_open`](https://github.com/base-rs/base-scheduler/blob/main/src/file_open.rs) | `(&Path, FileOptions, C) → OperationResult<File>` | Options expose read, write, create, create-new, and truncate; append is omitted. |
| [`FileSeek::file_seek`](https://github.com/base-rs/base-scheduler/blob/main/src/file_seek.rs) | `(F, SeekFrom, C) → OperationResult<u64>` | Set the shared cursor and return its new position. Serialize dependent cursor operations. |
| [`FileStat::file_stat`](https://github.com/base-rs/base-scheduler/blob/main/src/file_stat.rs) | `(F, C) → OperationResult<Metadata>` | Associated owned metadata; an observation rather than a globally atomic snapshot. |
| [`FileTruncate::file_truncate`](https://github.com/base-rs/base-scheduler/blob/main/src/file_truncate.rs) | `(F, length: u64, C) → OperationResult<()>` | Change logical size without changing the cursor. |
| [`FileSync::file_sync`](https://github.com/base-rs/base-scheduler/blob/main/src/file_sync.rs) | `(F, SyncMode, C) → OperationResult<()>` | Synchronize data or data plus metadata. Await prior writes first. Does not flush application buffers or synchronize a parent directory. |
| [`FileAllocate::file_allocate`](https://github.com/base-rs/base-scheduler/blob/main/src/file_allocate.rs) | `(F, offset: u64, length: u64, AllocationMode, C) → OperationResult<()>` | Reserve backing storage, optionally preserving logical size. Filesystem support and errors remain observable; not a durability barrier. |
| [`FileAdvice::file_advise`](https://github.com/base-rs/base-scheduler/blob/main/src/file_advice.rs) | `(F, offset: u64, length: u64, AdviceMode) → io::Result<()>` | Uncancellable hint, awaited through OS completion. Zero length means through EOF. |
| `file_advise_submit` | same arguments → `io::Result<()>` | Await admission only; execution errors are not returned. Accepted work survives abandonment. |
| [`FileClose::file_close`](https://github.com/base-rs/base-scheduler/blob/main/src/file_close.rs) | `(File by value) → io::Result<()>` | Consume exclusive ownership and await admission plus OS close. Finish operations and close/drop bound readers and writers first. Not retried after close errors. |
| `file_close_submit` | `(File by value) → io::Result<()>` | Admission only; accepted cleanup is drained by graceful shutdown. |

[`FileMetadata`](https://github.com/base-rs/base-scheduler/blob/main/src/file_metadata.rs) exposes synchronous `file_type() → Option<FileType>`, `size() → Option<u64>`, and `accessed()`, `modified()`, `created() → Option<SystemTime>`. Absence represents unavailable information. Backends can retain native metadata and compute accessors without eagerly copying every field.

## Socket creation, control, and resolution

| Capability/hook | Arguments → result | Semantics |
| --- | --- | --- |
| [`SocketOpen::socket_open`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_open.rs) | `(SocketDomain, SocketType, Option<SocketProtocol>, C) → OperationResult<Socket>` asynchronously | Open nonblocking, without explicit bind/listen/connect. `None` selects the OS default protocol. |
| [`SocketBind::socket_bind`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_bind.rs) | `(K, SocketAddress, C) → OperationResult<()>` asynchronously | Bind an existing socket. |
| [`SocketListen::socket_listen`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_listen.rs) | `(K, backlog: i32, C) → OperationResult<()>` asynchronously | Begin listening; OS backlog and implicit-bind rules remain observable. |
| [`SocketConnect::socket_connect`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_connect.rs) | `(K, SocketAddress, C) → OperationResult<()>` asynchronously | Connect; cancellation does not undo an attempted connection. |
| [`SocketAccept::socket_accept`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_accept.rs) | `(K, C) → OperationResult<(Socket, SocketAddress)>` asynchronously | Return a new socket and peer address. |
| [`SocketLocalAddress::socket_local_address`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_local_address.rs) / [`SocketPeerAddress::socket_peer_address`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_peer_address.rs) | `(K) → io::Result<SocketAddress>` synchronously | Inspect local or connected peer endpoint. |
| [`SocketShutdown::socket_shutdown`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_shutdown.rs) | `(K, Shutdown) → io::Result<()>` asynchronously | Shutdown read, write, or both directions through completion; no cancellation token. |
| `socket_shutdown_submit` | same arguments → `io::Result<()>` | Await admission only. |
| [`SocketClose::socket_close`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_close.rs) | `(Socket by value) → io::Result<()>` asynchronously | Close exclusive ownership through completion; no cancellation token. |
| `socket_close_submit` | `(Socket by value) → io::Result<()>` asynchronously | Await admission only. |
| [`SocketRegistration::register_socket`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_registration.rs) / `register_listener` | `(TcpStream)` / `(TcpListener) → io::Result<Socket>` synchronously | Transfer an existing standard-library resource into the backend. |
| [`SocketGetOption<O>::socket_get_option`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_get_option.rs) | `(K, option: O, C) → OperationResult<O::Value>` asynchronously | Query a typed option. |
| [`SocketSetOption<O>::socket_set_option`](https://github.com/base-rs/base-scheduler/blob/main/src/socket_set_option.rs) | `(K, option: O, value: O::Value, C) → OperationResult<()>` asynchronously | Set a typed option. |
| [`AddressResolution::address_resolve`](https://github.com/base-rs/base-scheduler/blob/main/src/address_resolution.rs) | `(name: Option<&str>, AddressService, AddressHints, C) → OperationResult<Addresses>` asynchronously | Owned associated resolution result; independent of connect policy. Preserve ordering, duplicates, socket type, protocol, and IPv6 scope. |

`SocketAddress` is the standard-library IP `SocketAddr`; this is not a Unix-domain or arbitrary native-address interface. `SocketOption` associates a `Value: Send`; supplied boolean options are `TcpNoDelay`, `SocketReuseAddress`, and `SocketKeepalive`. Implementations may support additional typed options independently.

`AddressService` selects a port or named service. Resolver errors are `AddressResolveError` carried inside `OperationError::Io`, preserving a portable category and native code when available. [`AddressInfoCollection::entries`](https://github.com/base-rs/base-scheduler/blob/main/src/address_info_collection.rs) returns borrowed `AddressInfoView` entries exposing `address`, `socket_type`, `protocol`, and explicit `to_address_info` materialization. The result owner can retain the original native address list.

## Cancellation, tasks, blocking work, and signals

| Hook | Arguments → result | Semantics |
| --- | --- | --- |
| `Cancellation::new`, `default` | `() → Cancellation` | Independent uncancelled signal. |
| `Cancellation::never` | `() → Cancellation` | Allocation-free signal that cannot be cancelled. |
| `clone`, `child` | `&Cancellation → Cancellation` | Clone shares the signal; a child receives ancestor cancellation but does not cancel ancestors or siblings. A child of `never` is independently cancellable. |
| `cancel` | `() → bool` | Synchronously request cancellation of this signal and descendants; true only on its first request. Does not await their work. |
| `is_cancelled`, `check` | `() → bool` / `Result<(), Cancelled>` | Observe the persistent request. Dropping a token never requests cancellation. |
| `is_never` | `() → bool` | Whether this signal can never be cancelled, allowing backends to omit cancellation registration. Children of `never` are independently cancellable. |
| `cancelled` | `() → ()` asynchronously | Wait for notification; dropping the wait removes its listener only. |
| `defer_cancel` | `(C, future, on_cancel: FnOnce()) → future output` | Invoke the synchronous callback once when cancellation is observed, then keep awaiting the protected future. Does not prevent the wrapper being dropped. |
| [`Spawn::spawn`](https://github.com/base-rs/base-scheduler/blob/main/src/spawn.rs) | `(future) → Result<Handle<T>, SpawnError>` synchronously | Register `Send + 'static` work before making it runnable. Handle owns execution lifetime; dropping it requests destruction after an active poll returns. Await yields `Result<T, TaskError>`. Closed admission rejects the future without polling it. |
| [`Blocking::spawn_blocking`](https://github.com/base-rs/base-scheduler/blob/main/src/blocking.rs) | `(FnOnce() → T, C) → Result<Handle<T>, BlockingError>` asynchronously | Await bounded admission, then return an owning completion handle. Queued plus running jobs count against capacity. Dropping a handle cannot interrupt a running closure. A worker checks cancellation before starting; running code can capture a token to cooperate. |
| [`Signals::signal_subscribe`](https://github.com/base-rs/base-scheduler/blob/main/src/signals.rs) | `(&[Signal], target: Notify<Signal>) → io::Result<Subscription>` synchronously | Associated signal identifiers and owning subscription. Dropping the subscription stops new notifications; in-progress dispatch can finish. Notifications can coalesce. |
| [`Notify::notify`](https://github.com/base-rs/base-scheduler/blob/main/src/notify.rs) | `(value) → ()` synchronously | Shared notification hook (`Notify<Value>: Send + Sync`); `Arc` targets, standard-library channel senders, and cancellation signals have implementations. Signal callbacks run outside raw signal handlers and scheduler locks. |

`TaskError` distinguishes cancellation from a retained panic payload. `SpawnError` reports closed admission or exhausted task identifiers. `BlockingError` distinguishes cancellation, closed admission, and worker-start I/O failure. Task ownership and cancellation signals are separate: background work needs an owner that remains alive, and the primitive interface supplies no implicit parent-child task group.

## Time

| Capability/hook | Arguments → result | Semantics |
| --- | --- | --- |
| [`Clock::now`](https://github.com/base-rs/base-scheduler/blob/main/src/clock.rs) | `() → Instant` synchronously | Monotonic time within this scheduler's clock domain; use it in generic code instead of the system clock. |
| `sleep_until` | `(deadline: Instant, C) → OperationResult<()>` | Wait for an absolute deadline. Pre-cancellation wins even if elapsed; once waiting, observed expiry wins a simultaneous cancellation. No yield is promised for an elapsed deadline. |
| `sleep` | `(duration: Duration, C) → OperationResult<()>` | Relative deadline starts at first poll. Unrepresentable deadlines return `InvalidInput`. |
| `timeout` | `(duration: Duration, operation: F) → Result<F::Output, TimeoutError>` | Default helper for a `Send` future. Polls the operation first; only a pending operation starts the timer. Completion wins ties. Expiry or clock failure drops the operation, preserving its own errors separately from timeout errors. |
| [`Timers::timer`](https://github.com/base-rs/base-scheduler/blob/main/src/timers.rs) | `() → Timer` synchronously | Infallible construction of scheduler-compatible reusable state, without registering a deadline or selecting a worker. |
| `timer_wait_until` | `(&mut Timer, deadline: Instant, C) → OperationResult<()>` | Reuse timer state with one waiter. Incompatible scheduler pairing is rejected. Cancellation after admission disarms; pre-cancellation leaves state unchanged. |

Dropping an ordinary clock wait releases its registration. Reusable timer waits deliberately retain state for later reuse, potentially retaining capacity and an old waker. Dropping the timer retires it; forgetting it can leak capacity. Reusable timers remain experimental. Deadlines from independently advancing clock domains are not interchangeable.

`Clock::timeout` returns the operation's output inside `Ok`, `TimeoutError::Elapsed` on expiry, or `TimeoutError::Clock` when sleep fails, including deadline overflow. Immediate completion needs no clock read or timer registration. The helper drops the losing future before returning and drops both futures if abandoned. It does not request cancellation or wait for independently retiring work; a timed-out owned I/O operation may still complete in its backend. No synchronous poll is preempted.

## Buffer pools and received storage

[`BufferPooling::buffer_pool`](https://github.com/base-rs/base-scheduler/blob/main/src/buffer_pooling.rs) takes `BufferPoolOptions { buffers, buffer_size, alignment }` and synchronously returns `io::Result<Pool>`. `buffers` is an exact shared budget independent of worker count. Alignment is a nonzero power of two; native metadata and padding can require additional memory. Configure readers with this pool to share a bounded retention budget across connections.

[`BufferPool`](https://github.com/base-rs/base-scheduler/blob/main/src/buffer_pool.rs) is `Clone + Send + Sync + 'static` with `Buffer: ReceiveBuffer`. Its synchronous `capacity`, `buffer_size`, and `alignment` describe the whole pool, not free capacity. `try_acquire() → Option<Buffer>` never waits. `acquire(C) → Result<Buffer, Cancelled>` waits at exhaustion without silently growing. Clones share capacity; live application leases, frozen views, and published I/O all count against it.

| Buffer/message method | Arguments → result | Semantics |
| --- | --- | --- |
| `SocketRecvMessage::address` | `(&message) → Option<impl SocketAddressView>` | Borrow source metadata in its backend representation. |
| `payload` | `(&message) → &[u8]` | Exactly completed bytes, excluding metadata and unused capacity. |
| `truncated`, `control_truncated` | `(&message) → bool` | Describe the receive independently of later payload edits. |
| `into_payload` | `(message by value) → Payload: IoBuf` | Transfer ownership of exactly the visible payload range. Borrowed address views cannot survive extraction. |
| `SocketRecvMessageMut::payload_mut` | `(&mut message) → &mut [u8]` | Exclusive parsing or decryption access. |
| `SocketAddressView::to_socket_address` | `(&view) → SocketAddress` | Explicit portable endpoint conversion preserving IPv6 scope and flow information. |
| `ReceiveBuffer::truncate` | `(&mut buffer, length: usize) → ()` | Narrow the initialized visible prefix without moving storage; oversized lengths panic. |
| `ReceiveBuffer::freeze` | `(buffer by value) → Frozen: IoBuf + Clone` | End mutable access and create independently retained immutable storage. Final ownership release permits reuse. |

`IoBuf` and `IoBufMut` are unsafe stable-storage contracts. Moving the owner must not move its initialized byte allocation or change the exposed range while an operation owns it. `Vec<u8>`, boxed storage, and suitable pool leases can satisfy this contract; inline byte arrays cannot. `IoBuf::slice(self, range)` retains an owned subrange without copying. `HeapBufferPool` provides `BufferLease` and `FrozenBuffer`; immutable views can retain subranges, potentially pinning an entire slot. Buffer representation and recycling policy remain implementation details.

## Backend composition and selector integration

Application protocols and adapters share the resource contracts above. There is no separate public dispatch, monitor, or receiver protocol. Concrete adapters keep reusable registration, request, and retirement state private; Native forwards public factories directly without adding an I/O wrapper. The conformance crate tests public resources. Backend unit tests cover ring migration, aliasing, and completion retirement.

The selector lifecycle hooks below live in `base-scheduler-selector`, not the portable interface. They let the Native executor drive independently selected I/O machinery.

| Hook | Arguments → result | Semantics |
| --- | --- | --- |
| [`Selector::close`](https://github.com/base-rs/base-scheduler/blob/main/crates/selector/src/selector.rs) | `() → ()` synchronously | Stop admission and notify waiters; retain accepted cleanup and kernel-accessible storage. Repeatable. |
| `Selector::shutdown` | `() → ()` asynchronously | Close admission and await all accepted work retiring, including abandoned futures. Keep drivers progressing while waiting. |
| [`SelectorDriver::enter`](https://github.com/base-rs/base-scheduler/blob/main/crates/selector/src/selector_driver.rs) | `(driver by value) → Worker` synchronously | Consume a movable `Send + 'static` driver into its owning worker; thread-bound routing is restored on worker destruction. |
| [`SelectorWorker::waker`](https://github.com/base-rs/base-scheduler/blob/main/crates/selector/src/selector_worker.rs) | `() → Waker` | Interrupt an event wait, including wake/sleep races; harmless after driver retirement. |
| `SelectorWorker::close` | `(&mut worker) → ()` | Begin retirement without synchronously waiting. |
| `SelectorWorker::turn` | `(&mut worker, Option<Duration>) → io::Result<bool>` | Publish a bounded completion batch and wait at most the timeout. Zero is nonblocking; `None` permits indefinite waiting. True means closed and drained. After waking tasks, return so the executor can poll them. |

Drivers publish results and wake tasks; they never poll application futures from completion processing. The executor manages runnable tasks, stealing, worker-local timers, and the timeout passed to `turn`. io\_uring uses one ring per worker; active operations stay with their original ring through retirement, while subsequent operations can follow a migrated task.

Constructors, `block_on`, worker counts, `selector()`, concrete admission `close`, and runtime `shutdown` belong to the chosen backend, not a universal scheduler trait. Tokio's adapter borrows an application-owned runtime which must remain alive and driven. Native owns its executor and composes selectors. Direct supplies a reference implementation. An application must keep execution services running while awaiting graceful cleanup.

The concrete io\_uring multishot receiver remains a standalone backend experiment. It is not required by the bound reader/writer contract and is not an application default.

## Child process waits

`ProcessWait` supplies `process_wait(process: ProcessId, flags: ProcessWaitFlags, C) → OperationResult<Option<ProcessWaitResult>>`. `ProcessWaitResult` contains the actual positive `pid: u32` and a standard-library `ExitStatus`. The associated selector is `i32` on Unix and `u32` on Windows.

Unix selectors follow `waitpid`: positive selects one child, `0` the current process group, `-1` any child, and values below `-1` the corresponding positive process group. Each call reports one child's status. Flags default to termination; `untraced` also reports stops and `continued` reports resumption. `nohang` returns `None` if matching children exist without a reportable state change. No matching child is an OS error. Windows supports individual process IDs and termination only; stopped/continued flags return `Unsupported`. Keep the original `Child` or another process handle alive through the wait so its ID remains valid before the lazy operation opens its own handle.

The future is lazy and pre-cancellation prevents reaping. Cancellation and shutdown never terminate the child. A started blocking fallback may keep waiting, reap after cancellation or abandonment, and delay shutdown until the child changes state. Once a status has been obtained it wins concurrent cancellation; abandoning a wait does not undo reaping. Cancel and await before handing the selected Unix children to another waiter. Dropping alone does not establish backend retirement; after abandonment, drain or shut down the backend before handing ownership to another reaper. Do not concurrently poll a retained wait during that handoff. Waiting by PID does not update `std::process::Child`'s cached status: after successful exit collection, drop that handle instead of waiting or controlling the old PID through it.
