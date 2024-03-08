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

set -e

if [ -z "$8" ]; then
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

# calculate SHA256 hash of extension binary
cat $ext > $ext.append

( command -v truncate && truncate -s -256 $ext.append ) || ( command -v gtruncate && gtruncate -s -256 $ext.append ) || exit 1

# (Optionally) Sign binary
if [ "$DUCKDB_EXTENSION_SIGNING_PK" != "" ]; then
  echo "$DUCKDB_EXTENSION_SIGNING_PK" > private.pem
  $script_dir/compute-extension-hash.sh $ext.append > $ext.hash
  openssl pkeyutl -sign -in $ext.hash -inkey private.pem -pkeyopt digest:sha256 -out $ext.sign
  rm -f private.pem
else
  # Default to 256 zeros
  dd if=/dev/zero of=$ext.sign bs=256 count=1
fi

# append signature to extension binary
cat $ext.sign >> $ext.append

# compress extension binary
if [[ $4 == wasm_* ]]; then
  brotli < $ext.append > "$ext.compressed"
else
  gzip < $ext.append > "$ext.compressed"
fi

set -e

# Abort if AWS key is not set
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "No AWS key found, skipping.."
    exit 0
fi

# Set dry run unless guard var is set
DRY_RUN_PARAM="--dryrun"
if [ "$DUCKDB_DEPLOY_SCRIPT_MODE" == "for_real" ]; then
  DRY_RUN_PARAM=""
fi

# upload versioned version
if [[ $7 = 'true' ]]; then
  if [ -z "$3" ]; then
    echo "extension-upload-single.sh called with upload_versioned=true but no extension version was passed"
    exit 1
  fi

  if [[ $4 == wasm* ]]; then
    aws s3 cp $ext.compressed s3://$5/$1/$2/$3/$4/$1.duckdb_extension.wasm $DRY_RUN_PARAM --acl public-read --content-encoding br --content-type="application/wasm"
  else
    aws s3 cp $ext.compressed s3://$5/$1/$2/$3/$4/$1.duckdb_extension.gz $DRY_RUN_PARAM --acl public-read
  fi
fi

# upload to latest version
if [[ $6 = 'true' ]]; then
  if [[ $4 == wasm* ]]; then
    aws s3 cp $ext.compressed s3://$5/$3/$4/$1.duckdb_extension.wasm $DRY_RUN_PARAM --acl public-read --content-encoding br --content-type="application/wasm"
  else
    aws s3 cp $ext.compressed s3://$5/$3/$4/$1.duckdb_extension.gz $DRY_RUN_PARAM --acl public-read
  fi
fi
