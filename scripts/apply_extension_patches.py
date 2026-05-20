#!/usr/bin/env python3
import sys
import subprocess
import os

PATCHED_BRANCH = "patched"


def raise_error(error_msg):
    sys.stderr.write(error_msg + "\n")
    sys.exit(1)


def git(*args, capture_output=False):
    return subprocess.run(
        ["git", *args],
        check=True,
        capture_output=capture_output,
        text=True,
    )


def apply_patch(patch_file, current_dir):
    args = ["patch", "-p1", "--forward", "-i", patch_file]

    try:
        subprocess.run(
            args,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        repro_args = args[:]
        repro_args[1:1] = ["-d", current_dir]

        print("Failed to apply patch")
        print("\nCommand to reproduce locally:")
        print(" ".join(repro_args))

        print("\nError output:")
        print(e.stderr.decode("utf-8"))

        print("\nStandard output:")
        print(e.stdout.decode("utf-8"))

        sys.exit(1)


def commit_patch(patch_name):
    git("add", "-A")
    git("commit", "-m", f"Apply patch: {patch_name}")


def ensure_clean_repo(directory):
    status = git("status", "--porcelain", capture_output=True).stdout.strip()

    if status:
        print("Detected local changes - aborting patch application\n")

        print("--------------------------------------------------")
        print("Generate a patch file using the following command:")
        print("--------------------------------------------------")
        print(f"(cd {os.getcwd()} && " f"git diff > {os.path.join(directory, 'fix.patch')})")
        print("--------------------------------------------------")

        raise_error(
            "Repository has uncommitted changes. " "Please commit or stash them before running patch application."
        )


def ensure_patched_branch():
    existing = git("branch", "--list", PATCHED_BRANCH, capture_output=True).stdout.strip()

    if existing:
        current = git("rev-parse", "--abbrev-ref", "HEAD", capture_output=True).stdout.strip()

        if current == PATCHED_BRANCH:
            git("checkout", "HEAD", "--detach")

        git("branch", "-D", PATCHED_BRANCH)

    git("checkout", "-b", PATCHED_BRANCH)


def main():
    if len(sys.argv) != 2:
        raise_error("Usage: apply_patches.py <patch_directory>")

    directory = sys.argv[1]

    if os.environ.get("DUCKDB_SKIP_APPLYING_PATCHES") == "1":
        return

    if not os.path.isdir(directory):
        raise_error(f"Patch directory does not exist: {directory}")

    patches = sorted(os.listdir(directory))

    for patch in patches:
        if not patch.endswith(".patch"):
            raise_error(f'Patch file {patch} found in directory {directory} ' 'does not end in ".patch"')

    if not patches:
        raise_error(f"\nERROR: Extension patching enabled, but no patches found in '{directory}'.")

    print(f"Applying patches in '{os.getcwd()}'")

    #
    # Abort if repo is not clean
    #
    ensure_clean_repo(directory)

    print(f"Creating branch '{PATCHED_BRANCH}'")
    ensure_patched_branch()

    #
    # Apply + commit patches
    #
    for patch in patches:
        patch_path = os.path.join(directory, patch)

        print(f"\nApplying patch: {patch}")

        apply_patch(patch_path, os.getcwd())
        commit_patch(patch)

        print(f"Committed patch: {patch}")

    print("\n--------------------------------------------------")
    print(f"All patches successfully applied to branch '{PATCHED_BRANCH}'")
    print("Working tree is clean")
    print("--------------------------------------------------")


if __name__ == "__main__":
    main()
