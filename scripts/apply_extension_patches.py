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
    sys.stderr.write(error_msg + '\n')
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


current_dir = os.getcwd()
print(f"Applying patches at '{current_dir}'")
print(f"Resetting patches in {directory}\n")
subprocess.run(["git", "clean", "-f"], check=True)
subprocess.run(["git", "reset", "--hard", "HEAD"], check=True)
# Apply each patch file using patch
ARGUMENTS = ["patch", "-p1", "--forward", "-i"]
for patch in patches:
    print(f"Applying patch: {patch}\n")
    arguments = []
    arguments.extend(ARGUMENTS)
    arguments.append(os.path.join(directory, patch))
    try:
        subprocess.run(arguments, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        arguments[1:1] = ['-d', current_dir]
        command = " ".join(arguments)
        print(f"Failed to apply patch, command to reproduce locally:\n{command}")
        print("\nError output:")
        print(e.stderr.decode('utf-8'))
        print("\nStandard output:")
        print(e.stdout.decode('utf-8'))
        print("Exiting")
        exit(1)
