#!/bin/sh

set -ex

# in this script, echo goes to /dev/null so as not to double up with set -x.

OUTPUT_DIR="tools/pythonpkg"

echo "Deleting all of the following **/*.pyi in ${OUTPUT_DIR}..." > /dev/null
find "${OUTPUT_DIR}" -name "*.pyi" -not -path "*/.eggs/*" -print -delete

# https://mypy.readthedocs.io/en/stable/stubgen.html

stubgen \
	--verbose \
	--package duckdb \
	--output "${OUTPUT_DIR}"
