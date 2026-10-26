#!/bin/bash

# Main extension uploading script

# Note: use the DUCKDB_DEPLOY_SCRIPT_MODE variable to disable dryrun mode

# Usage: ./extension-upload-single.sh <name> <extension_version> <duckdb_version> <architecture> <s3_bucket> <copy_to_latest> <copy_to_versioned> [<path_to_ext>]
# <name>                : Name of the extension
# <extension_version>   : Version (commit / version tag) of the extension
# <duckdb_version>      : Version (commit / version tag) of DuckDB
# <architecture>        : Architecture target of the extension binary
# <s3_bucket>           : S3 bucket to upload to
# <copy_to_latest>      : Set this as the latest version ("true" / "false", default: "false")
# <copy_to_versioned>   : Set this as a versioned version that will not be overwritten
# <path_to_ext>         : (optional) Search this path for the extension

set -euo pipefail

if [ -z "${8:-}" ]; then
	BASE_EXT_DIR="/tmp/extension"
else
	BASE_EXT_DIR="$8"
fi

if [[ $4 == wasm* ]]; then
  ext="$BASE_EXT_DIR/$1.duckdb_extension.wasm"
else
  ext="$BASE_EXT_DIR/$1.duckdb_extension"
fi

script_dir="$(dirname "$(readlink -f "$0")")"
private_key_file=""

cleanup() {
	if [ -n "$private_key_file" ]; then
		rm -f "$private_key_file"
	fi
}

trap cleanup EXIT

get_file_size() {
	if stat -c%s "$1" >/dev/null 2>&1; then stat -c%s "$1"; else stat -f%z "$1"; fi
}

# Read 32-byte metadata field $3 (0-based from block start $2); strip trailing NUL then trailing spaces.
field_at() {
	dd if="$1" bs=1 skip=$(( $2 + $3 * 32 )) count=32 status=none | LC_ALL=C tr -d '\000' | sed 's/[[:space:]]*$//'
}

# The empty signature slot was sized by append_metadata, so the size to strip comes from the file, not the key.
# Mirror the engine verifier's legacy-first, magic-keyed dual probe: metadata block (256 bytes) at file_size-512 then
# file_size-640; marker is field index 6 (reader numbering = FIELD7, block offset 32), magic is field 0 (FIELD1).
detect_signature_size() {
	local file="$1" size marker
	size=$(get_file_size "$file")
	if [ "$size" -ge 512 ] && [ "$(field_at "$file" $(( size - 512 )) 7)" = "4" ]; then
		marker=$(field_at "$file" $(( size - 512 )) 1)
		if [ -z "$marker" ] || [ "$marker" = "256" ]; then echo 256; return 0; fi
		echo "unrecognized signature-size marker '$marker' (legacy offset) in '$file'" >&2; return 1
	fi
	if [ "$size" -ge 640 ] && [ "$(field_at "$file" $(( size - 640 )) 7)" = "4" ]; then
		marker=$(field_at "$file" $(( size - 640 )) 1)
		if [ "$marker" = "384" ]; then echo 384; return 0; fi
		echo "unrecognized signature-size marker '$marker' (rsa-3072 offset) in '$file'" >&2; return 1
	fi
	echo "could not determine signature slot size from extension footer of '$file'" >&2
	return 1
}

# RSA PKCS#1 v1.5 signature length equals the key modulus (2048 bit -> 256, 3072 bit -> 384).
key_signature_size() {
	local bits
	bits=$(openssl pkey -in "$1" -noout -text 2>/dev/null | grep -oE '\(([0-9]+) bit' | grep -oE '[0-9]+' | head -n1) || true
	case "$bits" in
		2048) echo 256 ;;
		3072) echo 384 ;;
		*) echo "unsupported signing key size '${bits:-unknown}' bits: only 2048 (RSA-2048) and 3072 (RSA-3072) are supported" >&2; return 1 ;;
	esac
}

# The slot size baked into the artifact is authoritative for how many bytes to strip and re-append.
signature_size=$(detect_signature_size "$ext") || exit 1

if [ -n "${DUCKDB_EXTENSION_SIGNING_PK:-}" ]; then
	private_key_file=$(mktemp "${TMPDIR:-/tmp}/duckdb-extension-signing.XXXXXX.pem")
	printf '%s\n' "$DUCKDB_EXTENSION_SIGNING_PK" > "$private_key_file"
	key_size=$(key_signature_size "$private_key_file") || exit 1
	# The key must match the slot the build produced, otherwise signing would corrupt the footer.
	if [ "$key_size" != "$signature_size" ]; then
		echo "Signing key produces a ${key_size}-byte signature but the extension footer has a ${signature_size}-byte slot; rebuild the extension with a matching signature size"
		exit 1
	fi
fi

# calculate SHA256 hash of extension binary
cat "$ext" > "$ext.append"

( command -v truncate >/dev/null 2>&1 && truncate -s -"$signature_size" "$ext.append" ) || \
	( command -v gtruncate >/dev/null 2>&1 && gtruncate -s -"$signature_size" "$ext.append" ) || exit 1

# (Optionally) Sign binary
if [ -n "${DUCKDB_EXTENSION_SIGNING_PK:-}" ]; then
	"$script_dir/compute-extension-hash.sh" "$ext.append" > "$ext.hash"
	openssl pkeyutl -sign -in "$ext.hash" -inkey "$private_key_file" -pkeyopt digest:sha256 -out "$ext.sign"
	# Guard against a key that passed the bit check but emits a different-length signature (e.g. non-RSA).
	actual_sig_size=$(get_file_size "$ext.sign")
	if [ "$actual_sig_size" != "$signature_size" ]; then
		echo "Produced signature is ${actual_sig_size} bytes but the footer slot is ${signature_size} bytes"
		rm -f "$ext.sign" "$ext.hash" "$ext.append"
		exit 1
	fi
else
	# Default to signature_size zeros
	dd if=/dev/zero of="$ext.sign" bs="$signature_size" count=1 status=none
fi

# append signature to extension binary
cat "$ext.sign" >> "$ext.append"
rm "$ext.sign"
rm -f "$ext.hash"

if [ -n "${DUCKDB_EXTENSION_SIGNING_OUTPUT_DIR:-}" ]; then
	signed_output_file="$DUCKDB_EXTENSION_SIGNING_OUTPUT_DIR/$3/$4/$(basename "$ext")"
	mkdir -p "$(dirname "$signed_output_file")"
	cp "$ext.append" "$signed_output_file"
fi

# compress extension binary
if [[ $4 == wasm_* ]]; then
	brotli < "$ext.append" > "$ext.compressed"
else
	gzip < "$ext.append" > "$ext.compressed"
fi
rm "$ext.append"

set -e

if ! command -v rclone >/dev/null 2>&1; then
    case "$(uname -s)" in
      MINGW*|MSYS*|CYGWIN*)
        choco install rclone -y --limit-output --no-progress
        ;;
      *)
        install_runner=(bash)
        if command -v sudo >/dev/null 2>&1; then
          install_runner=(sudo bash)
        fi
        curl -fsSL --retry 5 https://rclone.org/install.sh | "${install_runner[@]}"
        ;;
    esac
fi

# Set dry run unless guard var is set
DRY_RUN_PARAM="--dry-run"
if [ "${DUCKDB_DEPLOY_SCRIPT_MODE:-}" == "for_real" ]; then
  DRY_RUN_PARAM=""
fi

# Abort if credentials are not set
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
	echo "Missing AWS credentials: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required"
	rm "$ext.compressed"
	if [ "$DRY_RUN_PARAM" == "" ]; then
		exit 1
	else
		exit 0
	fi
fi

dest_extension="gz"
extra_upload_args=()
if [[ $4 == wasm* ]]; then
  dest_extension="wasm"
  extra_upload_args=(
    --header-upload "Content-Encoding: br"
    --header-upload "Content-Type: application/wasm"
  )
fi

upload_extension() {
  local destination="$1"
  local s3_provider="${S3_PROVIDER:-AWS}"
  if [[ "${AWS_ENDPOINT_URL}" == *"r2.cloudflarestorage.com"* ]]; then
    s3_provider="Cloudflare"
  fi
  local rclone_s3_args=(
    --s3-provider "${s3_provider}"
    --s3-endpoint "${AWS_ENDPOINT_URL}"
  )

  # NOTE: flags largely try to simplify this to a PASS/FAIL PUT, skipping
  # checks that otherwise normally occur; HEADs are not allowed for this 
  # Write-Only token.
  # --no-check-dest -> no pre-write HEAD call
  # --s3-no-check-bucket -> don't check for bucket existence
  # --s3-no-head -> no post-write HEAD call/check
  #
  set -x
  rclone $DRY_RUN_PARAM copyto \
    --no-traverse \
    --ignore-times \
    --ignore-checksum \
    --no-check-dest \
    --s3-no-check-bucket \
    --s3-no-head \
    "${rclone_s3_args[@]}" \
    "${extra_upload_args[@]}" \
    "$ext.compressed" \
    ":s3,env_auth=true:${destination}"
}

# upload versioned version
if [[ $7 = 'true' ]]; then
  if [ -z "$3" ]; then
    echo "extension-upload-single.sh called with upload_versioned=true but no extension version was passed"
	rm "$ext.compressed"
	exit 1
  fi

  if [ "${DUCKDB_DEPLOY_SCRIPT_MODE:-}" == "for_real" ]; then
    upload_extension "$5/$1/$2/$3/$4/$1.duckdb_extension.$dest_extension"
  fi
fi

# upload to latest version
if [[ $6 = 'true' ]]; then
  if [ "${DUCKDB_DEPLOY_SCRIPT_MODE:-}" == "for_real" ]; then
    upload_extension "$5/$3/$4/$1.duckdb_extension.$dest_extension"
  fi
fi

# clean up
rm "$ext.compressed"
