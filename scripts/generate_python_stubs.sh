#!/bin/sh

set -ex

# in this script, echo goes to /dev/null so as not to double up with set -x.

OUTPUT_DIR="tools/pythonpkg/duckdb-stubs"

rm -rf "${OUTPUT_DIR}"

# https://mypy.readthedocs.io/en/stable/stubgen.html

stubgen \
	--verbose \
	--package duckdb \
	--output "${OUTPUT_DIR}"


# We need this while `duckdb` is a single file module and not a package.
# If `duckdb` becomes a proper package, this can be removed.
mv "${OUTPUT_DIR}/duckdb.pyi" "${OUTPUT_DIR}/__init__.pyi"
