set -euo pipefail
root=/tmp/base-http-post-submit-20261028
mkdir "$root"
mkdir "$root/scheduler" "$root/binaries" "$root/wrk"
cp /tmp/base-http-fixed-rate-20261028/binaries/server "$root/binaries/baseline"
cp /tmp/base-http-fixed-rate-20261028/binaries/client "$root/binaries/client"
cp /tmp/base-http-owned-receive-20261028/wrk/wrk "$root/wrk/wrk"
cp -a /tmp/base-http-owned-receive-20261028/source/http "$root/http"
cp /tmp/base-http-fixed-rate-20261028/run.py "$root/fixed-run.py"
cp /tmp/base-http-owned-receive-20261028/http-run.py "$root/http-run.py"
