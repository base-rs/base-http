#!/bin/bash
# Final interleaved counter comparison: main, the candidate, the candidate without the idle probe (diagnostic), Hyper.
set -e
cd ~/client-gap
for CASE in "c32:--concurrency 32" "c1:--concurrency 1" "tls-c32:--concurrency 32 --tls"; do
  name=${CASE%%:*}; extra=${CASE#*:}
  OUT=out/final-$name ROUNDS=${ROUNDS:-10} EXTRA="$extra" bash compare.sh \
    main=main:base-open head=head:base-open noprobe=diaghead:base-open:DIAG_NO_PROBE=1 hyper=main:hyper \
    > out/final-$name.log 2>&1
done
