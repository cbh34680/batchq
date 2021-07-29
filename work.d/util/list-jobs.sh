#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))

#set -eux -o pipefail +o posix

#jq_key=".additional.retval${1}"
jq_key=".additional"

echo
echo "# QUERY=[$jq_key]"
echo

cat << EOF | nc -v localhost 9999 | jq -r "$jq_key"
{"worker-key":"list-jobs", "exec-params":{"kwargs":{"key":"response", "order-by":"desc", "limit":999999}}}

EOF

exit 0
