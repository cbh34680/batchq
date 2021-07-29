#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))

#set -eux -o pipefail +o posix

if [[ -z $1 ]]
then
  jq_key=".additional.retval.vals"
else
  jq_key=".additional.retval.vals.\"${1}\""
fi

echo
echo "# QUERY=[$jq_key]"
echo

cat << EOF | nc -v localhost 9999 | jq -r "$jq_key"
{"worker-key":"dump-memory"}

EOF

exit 0
