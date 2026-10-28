set -euo pipefail
cd /tmp/base-http-fixed-rate-20261028
sha256sum --check expected-binaries.sha256 > preserved-binary-verification.log
tar --exclude=target -czf client-source.tar.gz client run.py
mkdir -p retained-binaries
gzip -c binaries/client > retained-binaries/client.gz
sha256sum client-source.tar.gz retained-binaries/client.gz > archive-sha256.txt
