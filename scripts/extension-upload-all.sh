#!/bin/bash

# Uploads all extensions found in <base_dir_glob> (default: build/release/extension/*)
# this script is used by DuckDB CI to upload all extensions at once

# Usage: ./extension-upload-all.sh <architecture> <duckdb_version> [<base_dir_glob>]

# The directory that the script lives in, thanks @Tishj
script_dir="$(dirname "$(readlink -f "$0")")"

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./extension-upload-all.sh <architecture> <duckdb_version> [<base_dir_glob>]"
    exit 1
fi

if [ -z "$3" ]; then
    BASE_DIR="build/release/extension/*"
else
    BASE_DIR="$3"
fi

set -e

# Ensure we do nothing on failed globs
shopt -s nullglob

# Print dry run / real run
if [ "$DUCKDB_DEPLOY_SCRIPT_MODE" == "for_real" ]; then
    echo "Deploying extensions.."
else
    echo "Deploying extensions.. (DRY RUN)"
fi

FILES="$BASE_DIR/*.duckdb_extension"
for f in $FILES
do
    ext_name=`basename $f .duckdb_extension`
    echo "found extension: '$ext_name'"

    # args: <name> <extension_version> <duckdb_version> <architecture> <s3_bucket> <copy_to_latest> <copy_to_versioned> [<path_to_ext>]
	  $script_dir/extension-upload-single.sh $ext_name "" "$2" "$1" "duckdb-extensions" true false "$(dirname "$f")"
done