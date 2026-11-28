#!/bin/bash
# Run a script under the shared host lock.
exec flock /tmp/base-benchmark-host.lock bash "$@"
