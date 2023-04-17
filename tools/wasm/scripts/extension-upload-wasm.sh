#!/bin/bash

# Usage: ./extension-upload.sh <architecture> <commithash or version_tag>

set -e

echo "$DUCKDB_EXTENSION_SIGNING_PK" > private.pem

FILES="build/$1/extensions/*/*.duckdb_extension.wasm"
for f in $FILES
do
	# validate input file
	wasm-validate --enable-all $f

	ext=`basename $f .duckdb_extension.wasm`
	echo $ext
	# calculate SHA256 hash of extension binary
	cat $f > $f.append
	# 0 for custom section
	echo -n -e '\x00' >> $.append
	# 113 in hex = 275 in decimal, total lenght of what follows (1 + 16 + 2 + 256)
	# [1(continuation) + 0010011(payload) = \x93, 0(continuation) + 10(payload) = \x02]
	echo -n -e '\x93\x02' >> $.append
	# 10 in hex = 16 in decimal, lenght of name, 1 byte
	echo -n -e '\x10' >> $.append
	# the name of the WebAssembly custom section, 16 bytes
	echo -n 'duckdb_signature' >> $.append
	# 100 in hex, 256 in decimal
	# [1(continuation) + 0000000(payload) = \x80, 0(continuation) + 10(payload) = \x02],
	# for a grand total of 2 bytes
	echo -n '\x80\x02' >> $.append
	# the actual payload, 256 bytes, to be added later

	openssl dgst -binary -sha256 $f.append > $f.hash
	# encrypt hash with extension signing private key to create signature
	openssl pkeyutl -sign -in $f.hash -inkey private.pem -pkeyopt digest:sha256 -out $f.sign
	# append signature to extension binary
	cat $f.sign >> $f.append

	# validate generated file
	wasm-validate --enable-all $f.append

	# compress extension binary
	gzip < $f.append > "$f.gz"
	# upload compressed extension binary to S3
	aws s3 cp $f.gz s3://duckdb-extensions/$2/wasm-$1/$ext.duckdb_extension.gz --acl public-read
done

rm private.pem
