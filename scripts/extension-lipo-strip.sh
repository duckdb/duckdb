#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: ./extension-lipo-strip.sh <arch_to_strip> <base_dir> <output_dir>"
fi

set -e

# Ensure we do nothing on failed globs
shopt -s nullglob

FILES="$2/*/*.duckdb_extension"

# Ensure the output dir exists
mkdir -p $3

# Give dem fat extensions some lipo
for f in $FILES
do
	ext=`basename $f .duckdb_extension`
	lipo $f -remove $1 -output $3/$ext.duckdb_extension
done