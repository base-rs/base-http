# Compile-time OOB reader policy

Hana Linux measurements and regression evidence, 27 October 2026. See [findings](../../oob-reader-option.md).

The retained scheduler change is in `scheduler.patch`, based on release v0.1.2. The feature revision is `da90905140213e169ea21ad6267f511ffdbbb077`. The feature worktree is `/Users/samuel/Developer/base-rs/base-scheduler-oob` on `feat/socket-stream-oob`. The original scheduler working tree and its 26 pending-file hashes are unchanged. The regular hello benchmark manifest continues to use registry 0.1.2; the staged Linux build patches all scheduler packages to the feature implementation.

`benchmark/` contains 27 interleaved six-second samples, with one-second warmup and response validation for each server instance. Three rounds compare frozen published epoll, default OOB-disabled epoll, and Hyper/Tokio at 1, 16 and 64 connections. CPU affinity, binary hashes and host details are in `benchmark/environment.json`. Medians of paired percentage changes are computed per round and can differ from ratios of throughput medians.

`default_oob_http_probe.py` primes an ordinary HTTP connection, pauses only its own test server to queue a complete request split by an urgent byte, then resumes it. Native epoll rejects the connection promptly. The conservative io_uring server reads the ordinary data and responds successfully. No further peer traffic is needed for either result.

`diagnostics/default-oob-ring-trace.log` is evidence from a rejected io_uring optimization: ordinary priming data produced an advisory completion mask `0x3` (`POLLIN | POLLPRI`), which would incorrectly reject an ordinary connection. That implementation is not in the retained patch. The retained io_uring implementation uses its existing conservative path; added tests exercise both compile-time selections and urgent marks.

Workspace tests and coverage use the repository Bake task. Package-only selector coverage omits downstream conformance exercise; full workspace coverage is the relevant gate. Logs distinguish successful checks from infrastructure failures and rejected experiments.

Full workspace coverage passed on macOS at 28,386/28,386 measured regions and on Linux at 39,734/39,734. Linux uses `/usr/bin/llvm-cov` and `/usr/bin/llvm-profdata`. The task-local `cargo-arch-coverage` wrapper reuses the completed instrumented tests and excludes only the external Arch prebuilt standard-library source prefix from reporting. It excludes no project files and retains Bake's 100% project region threshold. Its successful gate log is `diagnostics/default-oob-linux-workspace-coverage-gate.log`; the earlier unfiltered log records the missing standard-library source-path failure.
