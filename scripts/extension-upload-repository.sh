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

echo $BASE_DIR

set -e

# Ensure we do nothing on failed globs
shopt -s nullglob

if [ "$DUCKDB_DEPLOY_SCRIPT_MODE" == "for_real" ]; then
    echo "Deploying extensions to `$TARGET_BUCKET`.."
else
    echo "Deploying extensions to `$TARGET_BUCKET`.. (DRY RUN)"
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

        for f in $FILES; do
            if [[ $architecture == wasm* ]]; then
                ext_name=`basename $f .duckdb_extension.wasm`
            else
                ext_name=`basename $f .duckdb_extension`
            fi
            
            echo "Processing extension: $ext_name (architecture: $architecture, version: $duckdb_version, path: $f)"
            
            # args: <name> <extension_version> <duckdb_version> <architecture> <s3_bucket> <copy_to_latest> <copy_to_versioned> [<path_to_ext>]
            $script_dir/extension-upload-single.sh $ext_name "" "$duckdb_version" "$architecture" "$TARGET_BUCKET" true false "$(dirname "$f")"
        done
        echo ""
    done
done

