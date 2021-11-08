#!/bin/bash

FILES="build/release/extension/*/*.duckdb_extension"
for f in $FILES
do
	ext=`basename $f .duckdb_extension`
	echo $ext
	gzip -k -f $f
   	aws s3 cp $f.gz s3://duckdb-extensions/`git log -1 --format=%h`/$1/tpch.duckdb_extension.gz --acl public-read  
done