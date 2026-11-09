# Benchmark hosts

Each host that base-http's benchmarks are measured on has one document here, named after the host, such as [hana.md](hana.md). Benchmark readmes link to it rather than repeat its hardware or CPU placement.

A host document records:

- what the host is and how to reach it, and what else runs on it, by kind of service, without naming people;
- its hardware: CPU model, cores, threads and the SMT sibling layout, caches, base and turbo frequency, memory size and, where readable, type and speed, and NUMA nodes;
- its software and settings: frequency scaling driver, governor and energy preference, kernel, distribution and command line, CPU vulnerability mitigations, network interfaces and their interrupt placement, and loopback settings, since the benchmarks run over loopback, and the Rust toolchain used;
- the CPU placement for client and server benchmarks: which CPUs the client's workers, its controller, the server, and the runner and OS use, which SMT siblings stay idle, and the measurements that support it;
- the date it was gathered and the commands that gathered it, all readable without root, so that it can be gathered again.

Gather it again when the host's kernel, firmware, settings or toolchain change, and keep the previous values where results depend on them. A placement changes only with measurements behind it.
