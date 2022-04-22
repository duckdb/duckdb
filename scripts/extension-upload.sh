#!/bin/bash

set -e

FILES="build/release/extension/*/*.duckdb_extension"
for f in $FILES
do
	ext=`basename $f .duckdb_extension`
	echo $ext
	gzip < $f > "$f.gz"
	aws s3 cp $f.gz s3://duckdb-extensions/`git log -1 --format=%h`/$1/$ext.duckdb_extension.gz --acl public-read
done