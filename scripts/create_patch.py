import os
import argparse
import sys
import re
import subprocess
from typing import List, Dict
from pathlib import Path

SCRIPT_DIR = os.path.dirname(__file__)

parser = argparse.ArgumentParser(description="Generate a patch file for a DuckDB extension.")

parser.add_argument(
    "repository_path",
    type=str,
    help="Path to the repository where the changes live that should be turned into a patch.",
)

parser.add_argument(
    "extension_name",
    type=str,
    help="Name of the extension to patch, should match the name in `.github/config/extensions/<extension_name>.cmake`.",
)

parser.add_argument("patch_name", type=str, help="Name for the patch file to create.")

parser.add_argument("--overwrite", action="store_true", help="Allow overwriting the patch file if it already exists.")

args = parser.parse_args()


def verify_git_tag():
    # Locate the cmake file to extract the GIT_TAG from
    cmake_path = Path(SCRIPT_DIR) / '..' / ".github" / "config" / "extensions" / f"{args.extension_name}.cmake"
    if not cmake_path.is_file():
        print(f"Error: Extension CMake file not found: {cmake_path}")
        sys.exit(1)

    cmake_content = cmake_path.read_text()

    # Extract GIT_TAG from the cmake file
    match = re.search(r"\bGIT_TAG\s+([^\s\)]+)", cmake_content)
    if not match:
        print(f"Error: Could not find GIT_TAG in {cmake_path}")
        sys.exit(1)

    git_tag_in_cmake = match.group(1)

    # Get the current commit hash in repository_path
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=args.repository_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        current_commit = result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to run git in {args.repository_path} — {e.stderr.strip()}")
        sys.exit(1)

    # Compare the tags
    if git_tag_in_cmake != current_commit:
        print(
            f"Error: GIT_TAG in {cmake_path} is {git_tag_in_cmake}, "
            f"but repository {args.repository_path} is checked out at {current_commit}."
        )
        sys.exit(1)


def create_patch():
    # Collect changes with git diff
    try:
        diff_result = subprocess.run(
            ["git", "diff", "--ignore-submodules"],
            cwd=args.repository_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to run git diff — {e.stderr.strip()}")
        sys.exit(1)

    new_patch_content = diff_result.stdout
    if not new_patch_content.strip():
        print("⚠️ No changes detected in repository; no patch will be created.")
        sys.exit(0)

    def parse_patch_files_and_lines(patch_text):
        changes = {}
        current_file = None
        for line in patch_text.splitlines():
            if line.startswith("diff --git"):
                parts = line.split()
                if len(parts) >= 3:
                    # Format: diff --git a/file b/file
                    current_file = parts[2][2:]  # remove 'a/'
                    changes.setdefault(current_file, set())
            elif line.startswith("@@") and current_file:
                # Format: @@ -old_start,old_count +new_start,new_count @@
                m = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", line)
                if m:
                    start = int(m.group(1))
                    length = int(m.group(2) or "1")
                    for l in range(start, start + length):
                        changes[current_file].add(l)
        return changes

    new_changes = parse_patch_files_and_lines(new_patch_content)

    # Check conflicts with existing patches
    patch_dir = (Path(SCRIPT_DIR) / ".." / ".github" / "patches" / "extensions" / args.extension_name).resolve()
    patch_dir.mkdir(parents=True, exist_ok=True)

    for existing_patch in patch_dir.glob("*.patch"):
        if existing_patch.name == f"{args.patch_name}.patch":
            if not args.overwrite:
                print(f"A patch by the name '{args.patch_name}.patch' already exists, failed to create patch")
                sys.exit(1)
            else:
                continue
        existing_changes = parse_patch_files_and_lines(existing_patch.read_text())

        for file, lines in new_changes.items():
            if file in existing_changes:
                overlap = lines & existing_changes[file]
                if overlap:
                    print(f"❌ Conflict detected with existing patch: {existing_patch.name}")
                    print(f"   File: {file}")
                    print(f"   Overlapping lines: {sorted(overlap)}")
                    sys.exit(1)

    # Save patch file
    patch_dir = (Path(SCRIPT_DIR) / ".." / ".github" / "patches" / "extensions" / args.extension_name).resolve()
    patch_dir.mkdir(parents=True, exist_ok=True)

    patch_path = patch_dir / f"{args.patch_name}.patch"
    patch_path.write_text(diff_result.stdout)


verify_git_tag()

create_patch()
