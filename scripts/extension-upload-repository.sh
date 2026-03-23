#!/bin/bash

# Uploads all extensions found in <base_dir_glob> (default: build/release/extension/*)
# this script is used by DuckDB CI to upload all extensions at once

# Usage: ./extension-upload-all.sh <base_dir_glob>
# Expected directory structure: <base_dir_glob>/<duckdb_version>/<architecture>/

# The directory that the script lives in, thanks @Tishj
script_dir="$(dirname "$(readlink -f "$0")")"

if [ -z "$1" ]; then
    BASE_DIR="build/release/repository/*"
else
    BASE_DIR="$1"
fi

if [ -z "$2" ]; then
    TARGET_BUCKET="duckdb-core-extensions"
else
    TARGET_BUCKET="$2"
fi

set -e

# Ensure we do nothing on failed globs
shopt -s nullglob

if [ "$DUCKDB_DEPLOY_SCRIPT_MODE" == "for_real" ]; then
    echo "Deploying extensions from '$BASE_DIR' to bucket '$TARGET_BUCKET' .."
else
    echo "Deploying extensions from '$BASE_DIR' to bucket '$TARGET_BUCKET'.. (DRY RUN)"
fi

for version_dir in $BASE_DIR/*; do
    duckdb_version=$(basename "$version_dir")
    for arch_dir in "$version_dir"/*; do
        architecture=$(basename "$arch_dir")
        if [[ $architecture == wasm* ]]; then
            FILES="$arch_dir/*.duckdb_extension.wasm"
        else
            FILES="$arch_dir/*.duckdb_extension"
        fi

        echo ""

        for f in $FILES; do
            printf '%s\0%s\0%s\0' "$duckdb_version" "$architecture" "$f"
        done
    done
done | xargs -0 -n 3 -P "${CI_CPU_COUNT:-1}" bash -c '
    script_dir="$1"
    target_bucket="$2"
    duckdb_version="$3"
    architecture="$4"
    f="$5"

    if [[ $architecture == wasm* ]]; then
        ext_name=$(basename "$f" .duckdb_extension.wasm)
    else
        ext_name=$(basename "$f" .duckdb_extension)
    fi

    log_file=$(mktemp "${TMPDIR:-/tmp}/duckdb-extension-upload.XXXXXX") || exit 1
    trap '\''rm -f "$log_file"'\'' EXIT

    {
        echo "Processing extension: $ext_name (architecture: $architecture, version: $duckdb_version, path: $f)"
        "$script_dir/extension-upload-single.sh" "$ext_name" "" "$duckdb_version" "$architecture" "$target_bucket" true false "$(dirname "$f")"
        echo ""
    } >"$log_file" 2>&1

    cat "$log_file"
    rm -f "$log_file"
    trap - EXIT
' bash "$script_dir" "$TARGET_BUCKET"
