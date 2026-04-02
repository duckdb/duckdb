#!/usr/bin/env python3
"""Fix deprecated loop iterator syntax ${var} -> {var} in .test and .test_slow files."""

import re
import sys
from pathlib import Path


def fix_file(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    new_text = re.sub(r'\$\{(\w+)\}', r'{\1}', text)
    if new_text == text:
        return 0
    path.write_text(new_text, encoding="utf-8")
    return text.count('${') - new_text.count('${')


def main():
    root = Path(__file__).parent.parent / "test"
    files = sorted(root.rglob("*.test")) + sorted(root.rglob("*.test_slow"))
    total = len(files)
    fixed_files = 0
    fixed_occurrences = 0

    for i, path in enumerate(files, 1):
        rel = path.relative_to(root.parent)
        count = fix_file(path)
        if count:
            print(f"[{i}/{total}] Fixed {count} occurrence(s) in {rel}")
            fixed_files += 1
            fixed_occurrences += count

    print(f"\nDone: fixed {fixed_occurrences} occurrence(s) across {fixed_files} file(s).")


if __name__ == "__main__":
    main()
