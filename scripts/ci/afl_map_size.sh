#!/usr/bin/env bash
set -eu -o pipefail

fuzz_bin="build/fuzzer/test/unittest"

output="$(
	env -u AFL_MAP_SIZE -u AFL_DUMP_MAP_SIZE \
		"$fuzz_bin" --list-test-names-only </dev/null 2>&1
)"

map_size="$(printf '%s\n' "$output" | sed -nE 's/.*AFL_MAP_SIZE to ([0-9]+).*/\1/p' | head -n1)"

if [ -z "${map_size}" ]; then
	echo "Failed to derive AFL_MAP_SIZE from $fuzz_bin output" >&2
	exit 1
fi

printf '%s\n' "${map_size}"
