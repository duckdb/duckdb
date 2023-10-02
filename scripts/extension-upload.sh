#!/bin/bash

# Usage: ./extension-upload.sh <architecture> <commithash or version_tag> <(optionally) base_dir>

# The directory that the script lives in, thanks @Tishj
script_dir="$(dirname "$(readlink -f "$0")")"

if [ -z "$3" ]; then
    BASE_DIR="build/release/extension/*"
else
    BASE_DIR="$3"
fi

set -e

# Ensure we do nothing on failed globs
shopt -s nullglob

echo "$DUCKDB_EXTENSION_SIGNING_PK" > private.pem

FILES="$BASE_DIR/*.duckdb_extension"
for f in $FILES
do
	ext=`basename $f .duckdb_extension`
	# calculate SHA256 hash of extension binary
	$script_dir/compute-extension-hash.sh $f > $f.hash
	# encrypt hash with extension signing private key to create signature
	openssl pkeyutl -sign -in $f.hash -inkey private.pem -pkeyopt digest:sha256 -out $f.sign
	# append signature to extension binary
	cat $f.sign >> $f
	# compress extension binary
	gzip < $f > "$f.gz"
	# upload compressed extension binary to S3
	aws s3 cp $f.gz s3://duckdb-extensions/$2/$1/$ext.duckdb_extension.gz --acl public-read
done

rm private.pem
