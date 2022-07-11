#!/bin/sh

set -ex

# this script regenerates python stub files using
# https://mypy.readthedocs.io/en/stable/stubgen.html .
# Stubs are written to
OUTPUT_DIR="tools/pythonpkg/duckdb-stubs"
# which is installed as an auxilliary package in the duckdb egg.

# Unfortunately, stubgen is good but not quite perfect, and
# the stubs it generates need a little bit of tweaking, which
# this regeneration process will blow away. git add -p is your friend.
# To allow for this, please annotate any tweaks you subsequently
# make with something like
# # stubgen override
# .
# If you get particularly sick of this then there's a skeleton of
# a solution in https://stackoverflow.com/a/36510671/5264127
# but it might be overengineering things...


rm -rf "${OUTPUT_DIR}"


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
# to regenerate this from scratch, run scripts/regenerate_python_stubs.sh .
# be warned - currently there are still tweaks needed after this file is
# generated. These should be annotated with a comment like
# # stubgen override
# to help the sanity of maintainers.
EOF
	mv "$1.tmp" "$1"
)

find "${OUTPUT_DIR}" -name "*.pyi" -print |
	while read pyi_name
	do
		add_header "${pyi_name}"
	done

