#!/bin/bash

# Usage: ./extension-upload-wasm.sh <architecture> <commithash or version_tag>

set -e

# Ensure we do nothing on failed globs
shopt -s nullglob

if [[ -z "${DUCKDB_EXTENSION_SIGNING_PK}" ]]; then
	# no private key provided, use the test private key (NOT SAFE)
	# this is made so private.pem at the end of the block will be in
	# a valid state, and the rest of the signing process can be tested
	# even without providing the key
	cp test/mbedtls/private.pem private.pem
else
	# actual private key provided
	echo "$DUCKDB_EXTENSION_SIGNING_PK" > private.pem
fi

FILES="build/to_be_deployed/$2/$1/*.duckdb_extension.wasm"
for f in $FILES
do
        ext=`basename $f .duckdb_extension.wasm`
        echo $ext
        # calculate SHA256 hash of extension binary
        cat $f > $f.append
        # 0 for custom section
        # 113 in hex = 275 in decimal, total lenght of what follows (1 + 16 + 2 + 256)
        # [1(continuation) + 0010011(payload) = \x93, 0(continuation) + 10(payload) = \x02]
        echo -n -e '\x00' >> $f.append
        echo -n -e '\x93\x02' >> $f.append
        # 10 in hex = 16 in decimal, lenght of name, 1 byte
        echo -n -e '\x10' >> $f.append
        echo -n -e 'duckdb_signature' >> $f.append
        # the name of the WebAssembly custom section, 16 bytes
        # 100 in hex, 256 in decimal
        # [1(continuation) + 0000000(payload) = ff, 0(continuation) + 10(payload)],
        # for a grand total of 2 bytes
        echo -n -e '\x80\x02' >> $f.append
        # the actual payload, 256 bytes, to be added later
        scripts/compute-extension-hash.sh $f.append > $f.hash
        # encrypt hash with extension signing private key to create signature
        openssl pkeyutl -sign -in $f.hash -inkey private.pem -pkeyopt digest:sha256 -out $f.sign
        # append signature to extension binary
        cat $f.sign >> $f.append
        # compress extension binary
        brotli < $f.append > "$f.brotli"
        # upload compressed extension binary to S3
	if [[ -z "${AWS_SECRET_ACCESS_KEY}" ]]; then
		#AWS_SECRET_ACCESS_KEY is empty -> dry run
		aws s3 cp $f.brotli s3://duckdb-extensions/$2/$1/$ext.duckdb_extension.wasm --acl public-read --content-encoding br --content-type="application/wasm" --dryrun
	else
		aws s3 cp $f.brotli s3://duckdb-extensions/$2/$1/$ext.duckdb_extension.wasm --acl public-read --content-encoding br --content-type="application/wasm"
	fi
done

# remove private key
rm private.pem
