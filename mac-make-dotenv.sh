#!/bin/bash

#set -eu -o pipefail +o posix

cd -P $(dirname "$0")

cat << EOF > .env
PYTHONPATH=${PWD}/lib
EOF

exit 0
