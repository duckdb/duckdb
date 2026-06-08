#!/bin/bash

set -euo pipefail

if [ "$#" -ne 2 ]; then
	echo "Usage: $0 <signed_extensions_dir> <public_key_file>"
	exit 1
fi

signed_extensions_dir="$1"
public_key_file="$2"
script_dir="$(dirname "$(readlink -f "$0")")"
signature_size=256

get_file_size() {
	if stat -c%s "$1" >/dev/null 2>&1; then
		stat -c%s "$1"
	else
		stat -f%z "$1"
	fi
}

if [ ! -d "$signed_extensions_dir" ]; then
	echo "Signed extensions directory '$signed_extensions_dir' does not exist"
	exit 1
fi

if [ ! -f "$public_key_file" ]; then
	echo "Public key file '$public_key_file' does not exist"
	exit 1
fi

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/duckdb-extension-verify.XXXXXX")

cleanup() {
	rm -rf "$work_dir"
}

trap cleanup EXIT

verified_count=0

while IFS= read -r -d '' signed_extension; do
	relative_path="${signed_extension#"$signed_extensions_dir"/}"
	unsigned_extension="$work_dir/${relative_path}.unsigned"
	signature_file="$work_dir/${relative_path}.signature"
	hash_file="$work_dir/${relative_path}.hash"

	mkdir -p "$(dirname "$unsigned_extension")"
	file_size=$(get_file_size "$signed_extension")
	if [ "$file_size" -lt "$signature_size" ]; then
		echo "Signed extension '$signed_extension' is smaller than the signature footer"
		exit 1
	fi

	unsigned_size=$((file_size - signature_size))
	dd if="$signed_extension" of="$unsigned_extension" bs=1 count="$unsigned_size" status=none
	dd if="$signed_extension" of="$signature_file" bs=1 skip="$unsigned_size" count="$signature_size" status=none

	"$script_dir/compute-extension-hash.sh" "$unsigned_extension" > "$hash_file"
	openssl pkeyutl -verify -pubin -inkey "$public_key_file" -sigfile "$signature_file" -in "$hash_file" \
		-pkeyopt digest:sha256 >/dev/null

	echo "Verified signature: $relative_path"
	verified_count=$((verified_count + 1))
done < <(find "$signed_extensions_dir" -type f \( -name '*.duckdb_extension' -o -name '*.duckdb_extension.wasm' \) -print0 | sort -z)

if [ "$verified_count" -eq 0 ]; then
	echo "No signed extensions found under '$signed_extensions_dir'"
	exit 1
fi

echo "Verified $verified_count signed extensions"
