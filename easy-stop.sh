#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))

#set -eux -o pipefail +o posix

pidfile="work.d/sys/var/run/batchq.pid"

if [ -f $pidfile ]
then
  pidval=$(cat "$pidfile")

  echo "send signal to pid=${pidval}"
  kill -USR1 ${pidval}
fi

exit 0
