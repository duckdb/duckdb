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

if [[ $4 == wasm* ]]; then
  # 0 for custom section
  # 113 in hex = 275 in decimal, total lenght of what follows (1 + 16 + 2 + 256)
  # [1(continuation) + 0010011(payload) = \x93, 0(continuation) + 10(payload) = \x02]
  echo -n -e '\x00' >> $ext.append
  echo -n -e '\x93\x02' >> $ext.append
  # 10 in hex = 16 in decimal, lenght of name, 1 byte
  echo -n -e '\x10' >> $ext.append
  echo -n -e 'duckdb_signature' >> $ext.append
  # the name of the WebAssembly custom section, 16 bytes
  # 100 in hex, 256 in decimal
  # [1(continuation) + 0000000(payload) = ff, 0(continuation) + 10(payload)],
  # for a grand total of 2 bytes
  echo -n -e '\x80\x02' >> $ext.append
fi

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

if [ "$DUCKDB_EXTENSION_SIGNING_PK" != "" ]; then
  echo "$DUCKDB_EXTENSION_SIGNING_PK" > private.pem
  $script_dir/compute-extension-hash.sh $ext.append > $ext.hash
  openssl pkeyutl -sign -in $ext.hash -inkey private.pem -pkeyopt digest:sha256 -out $ext.sign
  rm -f private.pem
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
    aws s3 cp $ext.compressed s3://$5/duckdb-wasm/$1/$2/duckdb-wasm/$3/$4/$1.duckdb_extension.wasm $DRY_RUN_PARAM --acl public-read --content-encoding br --content-type="application/wasm"
  else
    aws s3 cp $ext.compressed s3://$5/$1/$2/$3/$4/$1.duckdb_extension.gz $DRY_RUN_PARAM --acl public-read
  fi
fi

# upload to latest version
if [[ $6 = 'true' ]]; then
  if [[ $4 == wasm* ]]; then
    aws s3 cp $ext.compressed s3://$5/duckdb-wasm/$3/$4/$1.duckdb_extension.wasm $DRY_RUN_PARAM --acl public-read --content-encoding br --content-type="application/wasm"
  else
    aws s3 cp $ext.compressed s3://$5/$3/$4/$1.duckdb_extension.gz $DRY_RUN_PARAM --acl public-read
  fi
fi
