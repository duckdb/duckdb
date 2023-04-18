#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/wasm_env.sh"

HELLO_WASM_CPP="${PROJECT_ROOT}/tools/wasm/hello_wasm.cpp"

test -f "${DUCKDB_WASM}" \
    && { echo "[ OK  ] DuckDB WASM: ${DUCKDB_WASM}"; } \
    || { echo "[ ERR ] DuckDB WASM: ${DUCKDB_WASM}"; exit 1; }

BUILD_TYPE=${1:-Release}
FLAGS=-O3
case $BUILD_TYPE in
  "Debug") FLAGS=-O0 -g ;;
  "RelWithDebInfo") FLAGS=-O2 -g ;;
   *) ;;
esac
echo "Build Type: ${BUILD_TYPE}"

set -x

source "${EMSDK_ENV}"

${EMCPP} \
    ${FLAGS} \
    -std=gnu++17 \
    -fexceptions \
    -D NDEBUG \
    -D DUCKDB_NO_THREADS=1 \
    -s WASM=1 \
    -s LLD_REPORT_UNDEFINED=1 \
    -s WARN_ON_UNDEFINED_SYMBOLS=1 \
    -s ALLOW_MEMORY_GROWTH=1 \
    -s USE_PTHREADS=0 \
    -s DISABLE_EXCEPTION_CATCHING=0 \
    -s MODULARIZE=1 \
    -s EXPORT_NAME='DuckDB' \
    -s EXPORTED_FUNCTIONS='[ _main, _HelloWasm ]' \
    -I ${PROJECT_ROOT}/src/include \
    -I ${PROJECT_ROOT}/third_party/concurrentqueue/ \
    ${DUCKDB_WASM} \
    ${HELLO_WASM_CPP} \
    -o ${BUILD_DIR}/hello_wasm.js