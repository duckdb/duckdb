#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
	echo "Usage: $0 <build_type>" >&2
	exit 1
fi

BUILD_TYPE="$1"
BUILD_DIR="build/$BUILD_TYPE"
ARTIFACT_ROOT="build/$BUILD_TYPE-artifact"
ARTIFACT_DIR="$ARTIFACT_ROOT/$BUILD_TYPE"
ARTIFACT_TARBALL="build/$BUILD_TYPE-artifact.tar.gz"

if [[ ! -d "$BUILD_DIR" ]]; then
	echo "Build directory '$BUILD_DIR' does not exist" >&2
	exit 1
fi

rm -rf "$ARTIFACT_ROOT"
rm -f "$ARTIFACT_TARBALL"
mkdir -p "$ARTIFACT_DIR"/test/extension "$ARTIFACT_DIR"/src

# Required by CI jobs that run the CLI from build/<type>/duckdb.
cp -av "$BUILD_DIR/duckdb" "$ARTIFACT_DIR"/

# Required by CI test jobs that run the prebuilt unittest binary.
cp -av "$BUILD_DIR/test/unittest" "$ARTIFACT_DIR"/test/

# Required by ADBC and other tests that need the shared library.
shopt -s nullglob
so_files=("$BUILD_DIR"/src/libduckdb.so*)
if ((${#so_files[@]} > 0)); then
	cp -av "${so_files[@]}" "$ARTIFACT_DIR"/src/
else
	echo "No $BUILD_DIR/src/libduckdb.so* files found"
fi

# Required by regression jobs that run the prebuilt benchmark runner.
if [[ -f "$BUILD_DIR/benchmark/benchmark_runner" ]]; then
	mkdir -p "$ARTIFACT_DIR"/benchmark
	cp -av "$BUILD_DIR/benchmark/benchmark_runner" "$ARTIFACT_DIR"/benchmark/
else
	echo "No $BUILD_DIR/benchmark/benchmark_runner file found"
fi

# Required by extension tests using __BUILD_DIRECTORY__/test/extension/*.duckdb_extension.
extension_files=("$BUILD_DIR"/test/extension/*.duckdb_extension)
if ((${#extension_files[@]} > 0)); then
	for extension in "${extension_files[@]}"; do
		cp -av "$extension" "$ARTIFACT_DIR"/test/extension/
	done
else
	echo "No $BUILD_DIR/test/extension/*.duckdb_extension files found"
fi

# Required by tests that use the local extension repository under the build directory.
if [[ -d "$BUILD_DIR/repository" ]]; then
	cp -a "$BUILD_DIR/repository" "$ARTIFACT_DIR"/
else
	echo "No $BUILD_DIR/repository directory found"
fi

echo "$BUILD_TYPE artifact includes:"
find "$ARTIFACT_DIR" -type f | sort | sed "s|^$ARTIFACT_DIR/|  |"
echo "$BUILD_TYPE artifact size:"
du -h -d 3 "$ARTIFACT_DIR" | sort -h

set -x

# Use a tarball so executable bits are preserved when passing build artifacts between jobs.
# Use -4 to balance compression ratio (small enough output size) with compression time (a few sec).
tar -C "$ARTIFACT_ROOT" -cf - "$BUILD_TYPE" | gzip -4 > "$ARTIFACT_TARBALL"

ls -lh "$ARTIFACT_TARBALL"
