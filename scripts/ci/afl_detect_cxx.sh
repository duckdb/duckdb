#!/usr/bin/env bash

set -euo pipefail

if [[ -n "${AFL_CC:-}" && -n "${AFL_CXX:-}" ]]; then
	AFL_LTO_CMAKE_VAR="${AFL_LTO_CMAKE_VAR:-}"
	if [[ "${AFL_CC}" == "afl-clang-lto" && "${AFL_CXX}" == "afl-clang-lto++" && -z "${AFL_LTO_CMAKE_VAR}" ]]; then
		AFL_LTO_CMAKE_VAR="-DCMAKE_LTO=full"
	fi
	printf 'AFL_CC=%q\nAFL_CXX=%q\nAFL_LTO_CMAKE_VAR=%q\n' "${AFL_CC}" "${AFL_CXX}" "${AFL_LTO_CMAKE_VAR}"
	exit 0
fi

if command -v afl-clang-lto >/dev/null 2>&1 && command -v afl-clang-lto++ >/dev/null 2>&1; then
	printf 'AFL_CC=%q\nAFL_CXX=%q\nAFL_LTO_CMAKE_VAR=%q\n' "afl-clang-lto" "afl-clang-lto++" "-DCMAKE_LTO=full"
	exit 0
fi

if command -v afl-clang-fast >/dev/null 2>&1 && command -v afl-clang-fast++ >/dev/null 2>&1; then
	printf 'AFL_CC=%q\nAFL_CXX=%q\nAFL_LTO_CMAKE_VAR=%q\n' "afl-clang-fast" "afl-clang-fast++" ""
	exit 0
fi

echo "Error: AFL++ compiler wrappers not found. Need afl-clang-lto/lto++ or afl-clang-fast/fast++ (run: brew install afl++)" >&2
exit 1
