#!/bin/bash

rm -f hash_concats
touch hash_concats

split -b 1M $1

FILES="x*"
for f in $FILES
do
	# sha256 a segment
	openssl dgst -binary -sha256 $f >> hash_concats
	rm $f
done

# sha256 the concatenation
openssl dgst -binary -sha256 hash_concats > hash_composite

cat hash_composite
