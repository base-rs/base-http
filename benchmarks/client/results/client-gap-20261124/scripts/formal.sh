#!/bin/bash
# Formal interleaved comparison through the benchmark's own runner (holds the host lock throughout).
set -e
cd ~/client-gap
BASE=$PWD/main/benchmarks/client/target/release/base-http-client-benchmark
HEAD=$PWD/head/benchmarks/client/target/release/base-http-client-benchmark
rm -rf main-sources
python3 main/benchmarks/client/run.py --binary $BASE --output main-sources --toolchain '' --repeats 0 > /dev/null
python3 head/benchmarks/client/run.py --binary $HEAD --output ${OUT:-formal} --toolchain '' \
  --modes reuse --repeats ${REPEATS:-10} --seconds ${SECS:-3} --warmup 1 --concurrency 1,32 --transports tcp,tls \
  --implementations ${IMPLS:-before-open,base-open,before-call,base-call,hyper} \
  --baseline-binary $BASE --baseline-sources main-sources/sources.tar.gz \
  --client-cpus 1 --server-cpus 2,3
