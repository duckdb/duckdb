#!/bin/bash

# This script applies patches matching glob, and will fail if none are found.
FILES="$1/*.patch"

shopt -s nullglob

FOUND_PATCHES=0

for f in $FILES;
  do echo $f; git apply $f;FOUND_PATCHES=1;
done

if [[ "$FOUND_PATCHES" -eq 0 ]]; then
  >&2 echo -e "\nERROR: Extension patching enabled, but no patches found in '$1'. Please make sure APPLY_PATCHES is only enabled when there are actually patches present. See .github/patches/extensions/README.md for more details.\n"
  exit 1
fi