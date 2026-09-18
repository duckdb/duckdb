#!/bin/bash

set -euo pipefail

input_file="$1"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/duckdb-extension-hash.XXXXXX")

cleanup() {
	rm -rf "$work_dir"
}

trap cleanup EXIT

hash_concats="$work_dir/hash_concats"
hash_composite="$work_dir/hash_composite"
split_prefix="$work_dir/chunk_"

touch "$hash_concats"
split -b 1M "$input_file" "$split_prefix"

for f in "${split_prefix}"*; do
	# SHA256 each segment independently, then hash the concatenation of those digests.
	openssl dgst -binary -sha256 "$f" >> "$hash_concats"
done

openssl dgst -binary -sha256 "$hash_concats" > "$hash_composite"
cat "$hash_composite"
