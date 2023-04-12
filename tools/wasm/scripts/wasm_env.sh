#!/usr/bin/env bash

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)" &> /dev/null

EMSCRIPTEN_VERSION="2.0.14"
EMSDK_VERSION="2.0.14"

WASM_WD="${PROJECT_ROOT}/.wasm/"
EMSDK_REPO_DIR="${WASM_WD}/emsdk/"
EMSDK_RELEASE_URL="https://github.com/emscripten-core/emsdk/archive/${EMSDK_VERSION}.tar.gz"
EMSDK_RELEASE_TARBALL="${WASM_WD}/${EMSDK_VERSION}.tar.gz"
EMSDK_TOOL="${EMSDK_REPO_DIR}/emsdk"
EMSDK_ENV="${EMSDK_REPO_DIR}/emsdk_env.sh"

EMSCRIPTEN_DIR="${EMSDK_REPO_DIR}/upstream/emscripten/"
EMCMAKE="${EMSCRIPTEN_DIR}/emcmake"
EMMAKE="${EMSCRIPTEN_DIR}/emmake"
EMCC="${EMSCRIPTEN_DIR}/emcc"
EMCPP="${EMSCRIPTEN_DIR}/em++"

BUILD_DIR="${PROJECT_ROOT}/.wasm/build"
DUCKDB_WASM="${BUILD_DIR}/duckdb.wasm"
