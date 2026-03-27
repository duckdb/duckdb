#!/usr/bin/env python3
"""Adds the make format-fix pre-commit hook to .git/hooks/pre-commit."""

import os

HOOK_MARKER = '# make format-fix hook #'
HOOK_PATH = '.git/hooks/pre-commit'


FORMAT_SCRIPT = 'duckdb/scripts/format.py' if os.path.isfile('duckdb/scripts/format.py') else 'scripts/format.py' # this is to support extension repos
FORMAT_CMD = f'python3 {FORMAT_SCRIPT} {{}} --fix --noconfirm'

HOOK_BODY = f"""\

# make format-fix hook #
# Get list of staged files
staged_files=$(git diff --cached --name-only)
# Run formatter on staged files
echo "$staged_files" | xargs -P 0 -I {{}} {FORMAT_CMD}
# Re-stage files that were:
# 1. Originally staged
# 2. Modified by the formatter
echo "$staged_files" | while read -r file; do
    if [ -n "$file" ] && git diff --name-only | grep -Fxq "$file"; then
        git add "$file"
    fi
done
"""


def main():
    if os.path.isfile(HOOK_PATH):
        with open(HOOK_PATH, 'r') as f:
            content = f.read()
        if HOOK_MARKER in content:
            print("Hook already exists, no action needed. exiting.")
        else:
            print(".git/hooks/pre-commit file exists but does not contain make `make format-fix` functionality for staged files")
            print("Adding `make format-fix` functionality for staged files")
            with open(HOOK_PATH, 'a') as f:
                f.write(HOOK_BODY)
    else:
        print("Creating .git/hooks/pre-commit file with `make format-fix` functionality for staged files")
        with open(HOOK_PATH, 'w') as f:
            f.write('#!/bin/sh\n' + HOOK_BODY)
        os.chmod(HOOK_PATH, 0o755)
    print("Done! ✅")


if __name__ == '__main__':
    main()