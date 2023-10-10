#!/usr/bin/env bash

set -euo pipefail

trap exit SIGINT

mkdir -p built_extensions
shopt -s nullglob
shopt -s globstar

FILES="extension/**/*.duckdb_extension"
for f in $FILES
do
        ext=`basename $f .duckdb_extension`
        echo $ext
        emcc $f -sSIDE_MODULE=1 -o built_extensions/$ext.duckdb_extension.wasm -O3
done

ls -la built_extensions
