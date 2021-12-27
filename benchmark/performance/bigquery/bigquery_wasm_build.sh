#!/usr/bin/env bash

set -euo pipefail

source "wasm_env.sh"

TEST_CPP="${PROJECT_ROOT}/bigquery/test.cpp"

FLAGS=-O2

set -x
BUILD_DIR="${PROJECT_ROOT}/build"
if [ -d ${BUILD_DIR} ]; then
    rm -r "${BUILD_DIR}"
fi
mkdir -p ${BUILD_DIR}

source "${EMSDK_ENV}"

${EMCPP} \
    ${FLAGS} \
    -s WASM=1 \
    -s EXPORTED_FUNCTIONS='[_even]' \
    ${TEST_CPP} \
    -o ${BUILD_DIR}/test.js

rm -f "a.out.js"
rm -f "a.out.wasm"