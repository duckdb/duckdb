#!/usr/bin/env bash
set -euo pipefail

AFLPP_DIR="./AFLplusplus"

rm -rf "${AFLPP_DIR}"
git clone --depth 1 --branch v4.32c https://github.com/AFLplusplus/AFLplusplus.git "${AFLPP_DIR}"

pushd "${AFLPP_DIR}"
echo "::group::Build afl++"
export CC="ccache clang"
export CXX="ccache clang++"

if [ -n "${CI:-}" ]; then
	export PREFIX="${AFLPP_ROOT:-/usr/local}"
fi
make source-only PERFORMANCE=1

if [ -n "${CI:-}" ]; then
	sudo make install
else
	make install
fi

echo "::endgroup::"
popd

rm -rf "${AFLPP_DIR}"

afl-fuzz --version
afl-clang-fast --version
