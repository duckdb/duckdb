#!/bin/sh

set -e
set -x

cd `dirname $0`

echo "R: configure"

if [ -z "${DUCKDB_R_DEBUG}" ]; then
  python3 rconfigure.py
else
  cd ../..
  GEN=ninja CONFIGURE_R=1 nice ${MAKE:-make} debug
fi
