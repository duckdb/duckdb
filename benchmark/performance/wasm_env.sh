#!/usr/bin/env bash

PROJECT_ROOT="."

EMSCRIPTEN_VERSION="2.0.14"
EMSDK_VERSION="2.0.14"

EMSDK_REPO_DIR="emsdk"
EMSDK_RELEASE_URL="https://github.com/emscripten-core/emsdk/archive/${EMSDK_VERSION}.tar.gz"
EMSDK_RELEASE_TARBALL="${EMSDK_VERSION}.tar.gz"
EMSDK_TOOL="${EMSDK_REPO_DIR}/emsdk"
EMSDK_ENV="emsdk_env.sh"

EMSCRIPTEN_DIR="${EMSDK_REPO_DIR}/upstream/emscripten/"
EMCMAKE="${EMSCRIPTEN_DIR}/emcmake"
EMMAKE="${EMSCRIPTEN_DIR}/emmake"
EMCC="${EMSCRIPTEN_DIR}/emcc"
EMCPP="${EMSCRIPTEN_DIR}/em++"

BUILD_DIR="${PROJECT_ROOT}/build"
