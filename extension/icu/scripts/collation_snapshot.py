#!/usr/bin/env python3
"""Records the sort keys of every collation, or compares two recordings.

Regenerating the collation tables from a new release of the Unicode data changes the sort
keys of the collations that the release touched. Recording a snapshot before the update and
comparing it afterwards shows which collations changed and where, so that the changes can be
checked against the release notes instead of being taken on trust.

    python3 extension/icu/scripts/collation_snapshot.py record before.tsv
    # regenerate the tables and rebuild
    python3 extension/icu/scripts/collation_snapshot.py record after.tsv
    python3 extension/icu/scripts/collation_snapshot.py compare before.tsv after.tsv

A snapshot holds one hash per collation per block of code points, which keeps it small while
still pointing at the characters that changed. Use --duckdb to pick the binary to record with,
it defaults to build/release/duckdb.
"""

import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# scripts -> icu -> extension -> the root of the repository
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
DEFAULT_DUCKDB = os.path.join(ROOT_DIR, "build", "release", "duckdb")

#! the code points of a snapshot row, small enough to point at a script or a block of one
BLOCK_SIZE = 4096
MAX_CODEPOINT = 0x110000


def query(duckdb, sql):
    out = subprocess.run([duckdb, "-noheader", "-list", "-c", sql], capture_output=True, text=True)
    if out.returncode != 0:
        raise SystemExit(out.stderr.strip())
    return [line.split("|") for line in out.stdout.strip().split("\n") if line]


def collations(duckdb):
    rows = query(duckdb, "select collname from pragma_collations() order by 1")
    # only the collations of the ICU extension have a collate_<name> function
    functions = {
        row[0][len("collate_") :]
        for row in query(
            duckdb, "select function_name from duckdb_functions() " "where function_name like 'collate\\_%' escape '\\'"
        )
    }
    return [row[0] for row in rows if row[0] in functions]


def record(duckdb, path):
    names = collations(duckdb)
    sys.stderr.write("recording %d collations with %s\n" % (len(names), duckdb))
    with open(path, "w") as f:
        for name in names:
            rows = query(
                duckdb,
                """
                select (i // {block})::int, md5(string_agg(icu_sort_key(chr(i::int), '{name}'), '' order by i))
                from range(1, {limit}) t(i) where i not between 55296 and 57343 group by 1 order by 1
            """.format(
                    block=BLOCK_SIZE, limit=MAX_CODEPOINT, name=name
                ),
            )
            for block, digest in rows:
                f.write("%s\t%s\t%s\n" % (name, block, digest))
    sys.stderr.write("wrote %s\n" % path)


def read_snapshot(path):
    snapshot = {}
    with open(path) as f:
        for line in f:
            name, block, digest = line.rstrip("\n").split("\t")
            snapshot[(name, int(block))] = digest
    return snapshot


def ranges(blocks):
    """Merges consecutive blocks into code point ranges."""
    result = []
    for block in sorted(blocks):
        start, end = block * BLOCK_SIZE, (block + 1) * BLOCK_SIZE - 1
        if result and result[-1][1] + 1 == start:
            result[-1][1] = end
        else:
            result.append([start, end])
    return result


def compare(before_path, after_path):
    before, after = read_snapshot(before_path), read_snapshot(after_path)
    names = sorted({name for name, _ in before} | {name for name, _ in after})
    changed = 0
    for name in names:
        if not any(key[0] == name for key in before):
            print("%-10s added" % name)
            changed += 1
            continue
        if not any(key[0] == name for key in after):
            print("%-10s removed" % name)
            changed += 1
            continue
        blocks = [
            block for (other, block), digest in before.items() if other == name and after.get((name, block)) != digest
        ]
        if not blocks:
            continue
        changed += 1
        spans = ", ".join("U+%04X..U+%04X" % (start, end) for start, end in ranges(blocks)[:8])
        print("%-10s %d blocks changed: %s%s" % (name, len(blocks), spans, " ..." if len(blocks) > 8 else ""))
    print("\n%d of %d collations changed" % (changed, len(names)))
    return 1 if changed else 0


def main():
    args = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
    duckdb = DEFAULT_DUCKDB
    for arg in sys.argv[1:]:
        if arg.startswith("--duckdb="):
            duckdb = arg[len("--duckdb=") :]
    if len(args) == 2 and args[0] == "record":
        record(duckdb, args[1])
        return 0
    if len(args) == 3 and args[0] == "compare":
        return compare(args[1], args[2])
    raise SystemExit(__doc__)


if __name__ == "__main__":
    sys.exit(main())
