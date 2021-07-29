#!/bin/bash

thisdir="$(dirname $(readlink -f "${BASH_SOURCE:-$0}"))"
cd "${thisdir}"

cat << EOF > .env
PYTHONPATH="${thisdir}/lib"
EOF

exit 0
