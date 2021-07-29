#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))

#set -eux -o pipefail +o posix

cat << EOF | nc -v localhost 9999 | jq -r .
{"worker-key":"dump-memory"}

EOF

exit 0
