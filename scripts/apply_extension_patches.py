#!/usr/bin/env python3
import sys
import glob
import subprocess
import os

# Get the directory and construct the patch file pattern
directory = sys.argv[1]
patch_pattern = f"{directory}*.patch"

# Find patch files matching the pattern
patches = glob.glob(patch_pattern)


def raise_error(error_msg):
    sys.stderr.write(error_message + '\n')
    sys.exit(1)


patches = sorted(os.listdir(directory))
for patch in patches:
    if not patch.endswith('.patch'):
        raise_error(
            f'Patch file {patch} found in directory {directory} does not end in ".patch" - rename the patch file'
        )


# Exit if no patches are found
if not patches:
    error_message = (
        f"\nERROR: Extension patching enabled, but no patches found in '{directory}'. "
        "Please make sure APPLY_PATCHES is only enabled when there are actually patches present. "
        "See .github/patches/extensions/README.md for more details."
    )
    raise_error(error_message)

print(f"Resetting patches in {directory}\n")
subprocess.run(["git", "log"], check=True)
subprocess.run(["git", "clean", "-f"], check=True)
subprocess.run(["git", "reset", "--hard", "HEAD"], check=True)
# Apply each patch file using patch
for patch in patches:
    print(f"Applying patch: {patch}\n")
    subprocess.run(["patch", "-p1", "--forward", "-i", os.path.join(directory, patch)], check=True)
