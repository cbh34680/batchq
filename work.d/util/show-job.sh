#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))

#set -eux -o pipefail +o posix

job="${1:-abc000-1}"

#jq_key=".additional.retval${1}"
jq_key=".additional"

cat << EOF | nc localhost 9999 | jq -r "$jq_key"
{"worker-key":"show-job", "exec-params":{"args":["${job}"], "kwargs":{"key":"response", "order-by":"desc", "limit":999999}}}

EOF

exit 0
