#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))

#set -eux -o pipefail +o posix

cat << EOF | nc -v localhost 9999
{"exec-params":{"args":[9, 8]}}

EOF

exit 0
