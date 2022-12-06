#!/bin/bash

# Usage: ./extension-upload.sh <architecture> <commithash or version_tag> <s3_bucket> <skip_signing>

set -e

SHOULD_SIGN=${4:-true}
S3_BUCKET=${3:-"duckdb-extensions"}

if [ "$SHOULD_SIGN" = true ] ; then
    echo "creating key"
    echo "$DUCKDB_EXTENSION_SIGNING_PK" > private.pem
fi

FILES="build/release/extension/*/*.duckdb_extension"
for f in $FILES
do
	ext=`basename $f .duckdb_extension`
	echo $ext

	if [ "$SHOULD_SIGN" = true ] ; then
	    echo "signing it"
      # calculate SHA256 hash of extension binary
      openssl dgst -binary -sha256 $f > $f.hash
      # encrypt hash with extension signing private key to create signature
      openssl pkeyutl -sign -in $f.hash -inkey private.pem -pkeyopt digest:sha256 -out $f.sign
      # append signature to extension binary
      cat $f.sign >> $f
  fi

  # compress extension binary
	gzip < $f > "$f.gz"
	# upload compressed extension binary to S3
	aws s3 cp $f.gz s3://$S3_BUCKET/$2/$1/$ext.duckdb_extension.gz --acl public-read
done

if [ "$SHOULD_SIGN" = true ] ; then
  rm private.pem
fi