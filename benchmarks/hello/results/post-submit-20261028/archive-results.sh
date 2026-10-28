set -euo pipefail
root=/tmp/base-http-post-submit-20261028
cd "$root"
if test -f regression-before-status.txt; then
  mv regression-before-status.txt regression-filter-mistake-status.txt
fi
sha256sum --check binaries.sha256
sha256sum --check archive-sha256.txt
tar -czf /tmp/base-http-post-submit-results-20261028.tar.gz --exclude=./scheduler --exclude=./http --exclude=./binaries --exclude=./wrk --exclude=./coverage-tools --exclude=./production.rs -C "$root" .
sha256sum /tmp/base-http-post-submit-results-20261028.tar.gz
