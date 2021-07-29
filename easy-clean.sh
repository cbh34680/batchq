#!/bin/bash

unalias -a
cd $(dirname $(readlink -f "${BASH_SOURCE:-$0}"))
#cd $(dirname $(readlink -f "$0"))

#set -eux -o pipefail +o posix

thisdir="${PWD}"

sysdir='work.d/sys'
etcdir="${sysdir}/etc"
vardir="${sysdir}/var"
varetcdir="${vardir}/etc"

rm -rf ${etcdir}
rm -rf ${vardir}
rm -rf work.d/app-example/*/log/


mkdir -p ${varetcdir}
echo '1.0' > ${varetcdir}/busy-threshold.val
echo '3' > ${varetcdir}/softlimit.val

if [[ -n ${WSL_DISTRO_NAME} ]]
then
  echo '127.0.0.1:9999' > ${varetcdir}/master-host.val

else
  echo '10.96.155.95:9999' > ${varetcdir}/master-host.val

fi

~/virtualenv/py38/bin/python ${sysdir}/make-config-py.txt --overwrite=True


exit 0
