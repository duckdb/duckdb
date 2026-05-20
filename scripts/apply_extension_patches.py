#!/usr/bin/env python3
import sys
import glob
import subprocess
import os
import tempfile

# Get the directory and construct the patch file pattern
directory = sys.argv[1]
patch_pattern = f"{directory}*.patch"

PATCHED_BRANCH = "patched"

if os.environ.get("DUCKDB_SKIP_APPLYING_PATCHES") == "1":
    exit(0)

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


def git(*args, capture_output=False):
    return subprocess.run(
        ["git", *args],
        check=True,
        capture_output=capture_output,
        text=True,
    )


current_dir = os.getcwd()

print(f"Applying patches at '{current_dir}'")
print(f"Resetting patches in {directory}\n")

# capture the current diff
diff_proc = git("diff", capture_output=True)
prev_diff = diff_proc.stdout

output_proc = git("diff", "--numstat", capture_output=True)
prev_output_lines = output_proc.stdout.split('\n')
prev_output_lines.sort()

# reset repository to clean state
git("clean", "-f")
git("reset", "--hard", "HEAD")

# create/reset patched branch
print(f"Creating/resetting branch '{PATCHED_BRANCH}'")

# delete existing branch if present
existing_branches = git("branch", "--list", PATCHED_BRANCH, capture_output=True).stdout.strip()
if existing_branches:
    current_branch = git("rev-parse", "--abbrev-ref", "HEAD", capture_output=True).stdout.strip()

    # switch away before deleting if currently checked out
    if current_branch == PATCHED_BRANCH:
        git("checkout", "HEAD", "--detach")

    git("branch", "-D", PATCHED_BRANCH)

# create and checkout fresh branch from current HEAD
git("checkout", "-b", PATCHED_BRANCH)


def apply_patch(patch_file):
    ARGUMENTS = ["patch", "-p1", "--forward", "-i"]
    arguments = []
    arguments.extend(ARGUMENTS)
    arguments.append(patch_file)

    try:
        subprocess.run(
            arguments,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
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


def commit_patch(patch_name):
    commit_message = f"Apply patch: {patch_name}"

    # stage all changes from the patch
    git("add", "-A")

    # create commit
    git("commit", "-m", commit_message)

    print(f"Committed patch: {patch_name}")


# Apply each patch file using patch
for patch in patches:
    print(f"Applying patch: {patch}\n")

    apply_patch(os.path.join(directory, patch))
    commit_patch(patch)


# all patches have applied - check the current diff
output_proc = git("diff", "--numstat", capture_output=True)
output_lines = output_proc.stdout.split('\n')
output_lines.sort()

if len(output_lines) <= len(prev_output_lines) and prev_output_lines != output_lines:
    print("Detected local changes - rolling back patch application")

    git("clean", "-f")
    git("reset", "--hard", "HEAD")

    with tempfile.NamedTemporaryFile() as f:
        f.write(prev_diff.encode("utf-8"))
        f.flush()
        apply_patch(f.name)

    print("--------------------------------------------------")
    print("Generate a patch file using the following command:")
    print("--------------------------------------------------")
    print(f"(cd {os.getcwd()} && git diff > {os.path.join(directory, 'fix.patch')})")
    print("--------------------------------------------------")

    exit(1)

print("\n--------------------------------------------------")
print(f"All patches successfully applied to branch '{PATCHED_BRANCH}'")
print("Working tree is clean.")
print("--------------------------------------------------")
