#!/bin/bash

set -euo pipefail

if [ "$#" -ne 2 ]; then
	echo "Usage: $0 <signed_extensions_dir> <public_key_file>"
	exit 1
fi

signed_extensions_dir="$1"
public_key_file="$2"
script_dir="$(dirname "$(readlink -f "$0")")"
# Minimum footer size; the per-file size (256/384/512) is detected from the footer marker.
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
file_list="$work_dir/signed_extensions.list"

cleanup() {
	rm -rf "$work_dir"
}

trap cleanup EXIT

find "$signed_extensions_dir" -type f \( -name '*.duckdb_extension' -o -name '*.duckdb_extension.wasm' \) -print0 | sort -z > "$file_list"

verified_count=$(tr -cd '\0' < "$file_list" | wc -c | tr -d '[:space:]')

if [ "$verified_count" -eq 0 ]; then
	echo "No signed extensions found under '$signed_extensions_dir'"
	exit 1
fi

cat "$file_list" | xargs -0 -n 1 -P "${CI_CPU_COUNT:-1}" bash -c '
	set -euo pipefail

	script_dir="$1"
	signed_extensions_dir="$2"
	public_key_file="$3"
	signature_size="$4"
	work_dir="$5"
	signed_extension="$6"

	get_file_size() {
		if stat -c%s "$1" >/dev/null 2>&1; then
			stat -c%s "$1"
		else
			stat -f%z "$1"
		fi
	}

	# Hex dump of 32-byte metadata field $3 (0-based from block start $2).
	field_hex() {
		dd if="$1" bs=1 skip=$(( $2 + $3 * 32 )) count=32 status=none | od -An -v -tx1 | tr -d '\'' \n'\''
	}

	# Hex of $1 NUL-padded to 32 bytes, the only accepted encoding of a metadata field.
	padded_hex() {
		local hex
		hex=$(printf '\''%s'\'' "$1" | od -An -v -tx1 | tr -d '\'' \n'\'')
		printf '\''%s'\'' "$hex"
		printf '\''%0*d'\'' $(( 64 - ${#hex} )) 0
	}

	# Mirror the engine verifier: probe each size legacy-first, block at size-256-S with magic (FIELD1) and marker (FIELD7) == S.
	detect_signature_size() {
		local file="$1" size="$2" probe block marker
		for probe in 256 384 512; do
			block=$(( size - 256 - probe ))
			if [ "$block" -lt 0 ] || [ "$(field_hex "$file" "$block" 7)" != "$(padded_hex 4)" ]; then continue; fi
			if [ "$probe" = 256 ]; then marker=""; else marker="$probe"; fi
			if [ "$(field_hex "$file" "$block" 1)" = "$(padded_hex "$marker")" ]; then echo "$probe"; return 0; fi
			echo "signature-size marker does not match the ${probe}-byte footer layout"; return 1
		done
		echo "could not locate extension metadata magic at any supported signature offset"
		return 1
	}

	relative_path="${signed_extension#"$signed_extensions_dir"/}"
	job_dir=$(mktemp -d "$work_dir/job.XXXXXX") || exit 1
	log_file=$(mktemp "$work_dir/log.XXXXXX") || exit 1
	trap '\''rm -rf "$job_dir" "$log_file"'\'' EXIT

	unsigned_extension="$job_dir/extension.unsigned"
	signature_file="$job_dir/extension.signature"
	hash_file="$job_dir/extension.hash"

	# Subshell so a failed step aborts only this file and its log is still printed by the handler below.
	if ! (
		file_size=$(get_file_size "$signed_extension")
		if [ "$file_size" -lt "$signature_size" ]; then
			echo "Signed extension '\''$signed_extension'\'' is smaller than the signature footer"
			exit 1
		fi

		if ! detected=$(detect_signature_size "$signed_extension" "$file_size"); then
			echo "Failed to detect signature size for '\''$signed_extension'\'': $detected"
			exit 1
		fi
		signature_size="$detected"

		unsigned_size=$((file_size - signature_size))
		head -c "$unsigned_size" "$signed_extension" > "$unsigned_extension"
		tail -c "$signature_size" "$signed_extension" > "$signature_file"

		"$script_dir/compute-extension-hash.sh" "$unsigned_extension" > "$hash_file" || exit 1
		openssl pkeyutl -verify -pubin -inkey "$public_key_file" -sigfile "$signature_file" -in "$hash_file" \
			-pkeyopt digest:sha256 >/dev/null || exit 1

		echo "Verified signature: $relative_path"
	) >"$log_file" 2>&1; then
		cat "$log_file"
		exit 1
	fi

	cat "$log_file"
	rm -rf "$job_dir" "$log_file"
	trap - EXIT
' bash "$script_dir" "$signed_extensions_dir" "$public_key_file" "$signature_size" "$work_dir"

echo "Verified $verified_count signed extensions"
