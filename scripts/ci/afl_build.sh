#!/usr/bin/env bash

set -euo pipefail

AFLPP_DIR="./AFLplusplus"

rm -rf "${AFLPP_DIR}"
git clone --depth 1 --branch v4.32c https://github.com/AFLplusplus/AFLplusplus.git "${AFLPP_DIR}"

pushd "${AFLPP_DIR}"
echo "::group::Build afl++"
CC="ccache clang" CXX="ccache clang++" make source-only PERFORMANCE=1
sudo make install
echo "::endgroup::"
popd

rm -rf "${AFLPP_DIR}"

afl-fuzz --version
afl-clang-fast --version
