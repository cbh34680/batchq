#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))

set -eux -o pipefail +o posix

cat << EOF | nc -v -U ../sys/var/run/batchq.sock
{"worker-key":"app1", "exec-params":{"args":[9, 5], "job":"abc000-1"}}

EOF

exit 0
