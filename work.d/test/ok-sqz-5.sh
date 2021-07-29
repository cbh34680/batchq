#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))

set -eux -o pipefail +o posix

\rm -f a.txt
\rm -f ../sys/var/tmp/splitter/a.txt

for i in $(seq 1 5)
do
  cat << EOF >> a.txt
{"worker-key":"app1", "exec-params":{"args":[${i}, $(( $RANDOM % 10 + 1 ))]}}
EOF

done

#cat a.txt > ../sys/var/tmp/splitter/
mv a.txt ../sys/var/tmp/splitter/

exit 0
