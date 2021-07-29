#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))

set -eu -o pipefail +o posix

now="$(date +'%y%m%d-%H%M%S')"

(
for i in $(seq 1 100)
do
  cat << EOF
{"worker-key":"app1", "exec-params":{"job":"${now}-${i}", "args":[${i}, $(( $RANDOM % 10 + 1 ))]}}
EOF

done

echo

) | nc -v -U ../sys/var/run/batchq.sock

exit 0
