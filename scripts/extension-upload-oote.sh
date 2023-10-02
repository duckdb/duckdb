#!/bin/bash

# Extension upload script for out of tree extensions

# Usage: ./extension-upload-oote.sh <name> <extension_version> <duckdb_version> <architecture> <s3_bucket> <copy_to_latest>
# <name>                : Name of the extension
# <extension_version>   : Version (commit / version tag) of the extension
# <duckdb_version>      : Version (commit / version tag) of DuckDB
# <architecture>        : Architecture target of the extension binary
# <s3_bucket>           : S3 bucket to upload to
# <copy_to_latest>      : Set this as the latest version ("true" / "false", default: "false")

set -e

ext="build/release/extension/$1/$1.duckdb_extension"

script_dir="$(dirname "$(readlink -f "$0")")"

# Abort if AWS key is not set
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "No AWS key found, skipping.."
    exit 0
fi

# (Optionally) Sign binary
if [ "$DUCKDB_EXTENSION_SIGNING_PK" != "" ]; then
  echo "$DUCKDB_EXTENSION_SIGNING_PK" > private.pem
  $script_dir/compute-extension-hash.sh $ext > $ext.hash
  openssl pkeyutl -sign -in $ext.hash -inkey private.pem -pkeyopt digest:sha256 -out $ext.sign
  cat $ext.sign >> $ext
fi

set -e

# compress extension binary
gzip < "${ext}" > "$ext.gz"

# upload compressed extension binary to S3
aws s3 cp $ext.gz s3://$5/$1/$2/$3/$4/$1.duckdb_extension.gz --acl public-read

# upload to latest if copy_to_latest is set to true
if [[ $6 = 'true' ]]; then
  aws s3 cp $ext.gz s3://$5/$1/latest/$3/$4/$1.duckdb_extension.gz --acl public-read
fi

if [ "$DUCKDB_EXTENSION_SIGNING_PK" != "" ]; then
  rm private.pem
fi