#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))
#cd $(dirname $(readlink -f "$0"))

#set -eux -o pipefail +o posix

thisdir="${PWD}"
server="${thisdir}/main/server.py"
export BQ_APP_WORK="${thisdir}/work.d"

(
  cd $BQ_APP_WORK/sys/
  ~/virtualenv/py38/bin/python make-config-py.txt
  PYTHONPATH="${thisdir}/lib" ~/virtualenv/py38/bin/python "${server}" --config="etc/config.json"
)

exit 0
