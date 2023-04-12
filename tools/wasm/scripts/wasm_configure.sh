#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/wasm_env.sh"

curl --version \
    && { echo "[ OK  ] Command: curl"; } \
    || { echo "[ ERR ] Command: curl"; exit 1; }

tar --version \
    && { echo "[ OK  ] Command: tar"; } \
    || { echo "[ ERR ] Command: tar"; exit 1; }

# Get the release archive
mkdir -p ${WASM_WD}
if [ ! -f "${EMSDK_RELEASE_TARBALL}" ]; then
    echo "[ RUN ] Download: ${EMSDK_RELEASE_URL}"
    curl -Lo "${EMSDK_RELEASE_TARBALL}" "${EMSDK_RELEASE_URL}"
fi
echo "[ OK  ] Release Archive: ${EMSDK_RELEASE_TARBALL}"

# Unpack the release archive
if [ ! -f "${EMSDK_TOOL}" ]; then
    set -x
    mkdir -p "${EMSDK_REPO_DIR}"
    tar -xvzf "${EMSDK_RELEASE_TARBALL}" -C "${EMSDK_REPO_DIR}" --strip-components=1
    set +x
fi
echo "[ OK  ] Unpacked Release: ${EMSDK_REPO_DIR}"

# Install & activate the emscripten version
set +x
${EMSDK_TOOL} install ${EMSCRIPTEN_VERSION}
${EMSDK_TOOL} activate ${EMSCRIPTEN_VERSION}