#!/usr/bin/env bash

set -euo pipefail

ARTIFACT_ROOT=build/relassert-artifact
ARTIFACT_DIR="$ARTIFACT_ROOT/relassert"
ARTIFACT_TARBALL=build/relassert-artifact.tar.gz

rm -rf "$ARTIFACT_ROOT"
rm -f "$ARTIFACT_TARBALL"
mkdir -p "$ARTIFACT_DIR"/test/extension "$ARTIFACT_DIR"/src

# Required for linux-debug-tests invocations.
cp -av build/relassert/test/unittest "$ARTIFACT_DIR"/test/

# Required by ADBC tests in linux-debug-tests.
shopt -s nullglob
so_files=(build/relassert/src/libduckdb.so*)
if ((${#so_files[@]} > 0)); then
	cp -av "${so_files[@]}" "$ARTIFACT_DIR"/src/
else
	echo "No build/relassert/src/libduckdb.so* files found"
fi

# Required by extension tests using __BUILD_DIRECTORY__/test/extension/*.duckdb_extension.
extension_files=(build/relassert/test/extension/*.duckdb_extension)
if ((${#extension_files[@]} > 0)); then
	for extension in "${extension_files[@]}"; do
		cp -av "$extension" "$ARTIFACT_DIR"/test/extension/
	done
else
	echo "No build/relassert/test/extension/*.duckdb_extension files found"
fi

# Required by tests that use the local extension repository under the build directory.
if [[ -d build/relassert/repository ]]; then
	cp -a build/relassert/repository "$ARTIFACT_DIR"/
else
	echo "No build/relassert/repository directory found"
fi

echo "Relassert artifact includes:"
find "$ARTIFACT_DIR" -type f | sort | sed "s|^$ARTIFACT_DIR/|  |"
echo "Relassert artifact size:"
du -h -d 3 "$ARTIFACT_DIR" | sort -h

set -x

# Use a tarball so executable bits are preserved when passing build artifacts between jobs.
# Use -4 to balance compression ratio (small enough output size) with compression time (a few sec).
tar -C "$ARTIFACT_ROOT" -cf - relassert | gzip -4 > "$ARTIFACT_TARBALL"

ls -lh "$ARTIFACT_TARBALL"
