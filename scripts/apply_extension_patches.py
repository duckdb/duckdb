#!/usr/bin/env python3
import sys
import glob
import subprocess
import os
import tempfile

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

# capture the current diff
diff_proc = subprocess.run(["git", "diff"], capture_output=True, check=True)
prev_diff = diff_proc.stdout

output_proc = subprocess.run(["git", "diff", "--numstat"], capture_output=True, check=True)
prev_output_lines = output_proc.stdout.decode('utf8').split('\n')
prev_output_lines.sort()

subprocess.run(["git", "clean", "-f"], check=True)
subprocess.run(["git", "reset", "--hard", "HEAD"], check=True)


def apply_patch(patch_file):
    ARGUMENTS = ["patch", "-p1", "--forward", "-i"]
    arguments = []
    arguments.extend(ARGUMENTS)
    arguments.append(patch_file)
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


# Apply each patch file using patch
for patch in patches:
    print(f"Applying patch: {patch}\n")
    apply_patch(os.path.join(directory, patch))

# all patches have applied - check the current diff
output_proc = subprocess.run(["git", "diff", "--numstat"], capture_output=True, check=True)
output_lines = output_proc.stdout.decode('utf8').split('\n')
output_lines.sort()

if len(output_lines) <= len(prev_output_lines) and prev_output_lines != output_lines:
    print("Detected local changes - rolling back patch application")

    subprocess.run(["git", "clean", "-f"], check=True)
    subprocess.run(["git", "reset", "--hard", "HEAD"], check=True)
    with tempfile.NamedTemporaryFile() as f:
        f.write(prev_diff)
        apply_patch(f.name)

    print("--------------------------------------------------")
    print("Generate a patch file using the following command:")
    print("--------------------------------------------------")
    print(f"(cd {os.getcwd()} && git diff > {os.path.join(directory, 'fix.patch')})")
    print("--------------------------------------------------")

    exit(1)
