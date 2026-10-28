# Candidate fixture adjustments

The first full candidate io_uring run passed 233 tests and failed two tests which depended on non-PollAdd requests remaining staged after the first nonblocking turn:

- `process_wait::tests::unsubmitted_driver_drop_reports_an_error_and_unknown_kernel_access_is_retained`: an invalid PID's WaitId completed and was collected during the newly immediate submit, so the in-flight count was zero rather than one.
- `multishot::tests::shutdown_during_buffer_exhaustion_retires_without_a_transport_error`: the newly immediate submit collected CQEs before the fixture's direct ring wait, so its bounded wait for three uncollected CQEs timed out.

Both now inject a supported submission interruption before their direct in-flight/CQ assertions. This establishes the state each fixture needs independently of batching policy. The process fixture deliberately checks conservative retention after an interrupted entry; it does not claim that the kernel has actually accessed its siginfo storage. The subsequent full run passes. The original failed log was overwritten by the rerun; this note records the observed failure without claiming it is a retained raw log.
