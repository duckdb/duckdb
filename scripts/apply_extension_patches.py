#!/usr/bin/env python3
import sys
import glob
import subprocess

# Get the directory and construct the patch file pattern
directory = sys.argv[1]
patch_pattern = f"{directory}*.patch"

# Find patch files matching the pattern
patches = glob.glob(patch_pattern)

# Exit if no patches are found
if not patches:
    error_message = (
        f"\nERROR: Extension patching enabled, but no patches found in '{directory}'. "
        "Please make sure APPLY_PATCHES is only enabled when there are actually patches present. "
        "See .github/patches/extensions/README.md for more details.\n"
    )
    sys.stderr.write(error_message)
    sys.exit(1)

# Apply each patch file using git apply
for patch in patches:
    print(f"Applying patch: {patch}")
    subprocess.run(["git", "apply", "--ignore-space-change", "--ignore-whitespace", patch])
