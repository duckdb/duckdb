#!/bin/bash

# Usage: ./extension-upload-wasm.sh <architecture> <commithash or version_tag>

# The directory that the script lives in, thanks @Tishj
script_dir="$(dirname "$(readlink -f "$0")")"

set -e

# Ensure we do nothing on failed globs
shopt -s nullglob

if [ -z "$DUCKDB_EXTENSION_SIGNING_PK" ]; then
	# Use test signature
	cp extension/test_signature.pem private.pem
else
	# Use provided signature
	echo "$DUCKDB_EXTENSION_SIGNING_PK" > private.pem
fi

DUCKDB=$script_dir/../build/release/duckdb

FILES="loadable_extensions/*.duckdb_extension.wasm"
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
        $script_dir/compute-extension-hash.sh $f.append > $f.hash
        # encrypt hash with extension signing private key to create signature
        openssl pkeyutl -sign -in $f.hash -inkey private.pem -pkeyopt digest:sha256 -out $f.sign
        # append signature to extension binary
        cat $f.sign >> $f.append
        if [ -f "$DUCKDB" ]; then
                $DUCKDB -c "LOAD '$f.append'" 2> log_signed || true
                $DUCKDB -unsigned -c "LOAD '$f.append'" 2> log_unsigned || true
                diff -q log_unsigned log_signed
                echo "Extension $ext signature verified"
        fi
        # compress extension binary
        brotli < $f.append > "$f.brotli"
        # upload compressed extension binary to S3
        if [ ! -z "$DUCKDB_EXTENSION_SIGNING_PK" ]; then
                aws s3 cp $f.brotli s3://duckdb-extensions/duckdb-wasm/$2/$1/$ext.duckdb_extension.wasm --acl public-read --content-encoding br --content-type="application/wasm"
                echo "Extension $ext uploaded"
        fi
done

rm private.pem

