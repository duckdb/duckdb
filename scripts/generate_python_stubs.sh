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

add_header() (
	{ set +x; } 2>/dev/null
	cat - "$1" > "$1.tmp" <<EOF
# AUTOMATICALLY GENERATED FILE
# to regenerate, run scripts/generate_python_stubs.sh
EOF
	mv "$1.tmp" "$1"
)

find "${OUTPUT_DIR}" -name "*.pyi" -print |
	while read pyi_name
	do
		add_header "${pyi_name}"
	done

