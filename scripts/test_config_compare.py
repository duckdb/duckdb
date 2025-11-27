import json
from collections import defaultdict
import sys

def load_skip_dict(path):
    """Load a skip_tests list into a dict: reason -> set(paths)."""
    with open(path) as f:
        data = json.load(f)

    out = {}
    for block in data.get("skip_tests", []):
        reason = block["reason"]
        paths = set(block.get("paths", []))
        out[reason] = paths
    return out


def compare_files(file_a, file_b):
    a = load_skip_dict(file_a)
    b = load_skip_dict(file_b)

    all_reasons = set(a.keys()) | set(b.keys())

    for reason in sorted(all_reasons):
        paths_a = a.get(reason, set())
        paths_b = b.get(reason, set())

        added = sorted(paths_b - paths_a)
        removed = sorted(paths_a - paths_b)

        if not added and not removed:
            continue

        print(f"\n=== Reason: {reason} ===")

        if removed:
            print("  - Present in A but NOT in B:")
            for p in removed:
                print(f"      {p}")

        if added:
            print("  + Present in B but NOT in A:")
            for p in added:
                print(f"      {p}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_skip_tests.py <fileA.json> <fileB.json>")
        exit(1)

    compare_files(sys.argv[1], sys.argv[2])
