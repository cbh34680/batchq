#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))

set -eux -o pipefail +o posix

cat << EOF | nc -v -U ../sys/var/run/batchq.sock
{"worker-key":"app2", "exec-params":{"args":[10], "job":"app2-$(date +'%y%m%d')"}}

EOF

exit 0
