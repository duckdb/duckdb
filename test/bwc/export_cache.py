#!/usr/bin/env python3
"""
Exports the BWC test cache from runtime directories.

For each version, creates a tar.gz archive containing only the files needed
for cached test runs:
  - *.sql (generated query files)
  - *.plan.bin (serialized execution plans)
  - *.<version>.result.bin (old version results)
  - fixtures/ directories (test fixture databases)

Usage:
  python3 test/bwc/export_cache.py [--version v1.4.4] [--output-dir /path/to/output]

By default, exports all versions found in the runtime directory.
"""

import argparse
import os
import tarfile
import sys
from os.path import abspath, dirname

parser = argparse.ArgumentParser(description='Export BWC test cache from runtime directories')
parser.add_argument('--version', dest='version', help='Export cache for a specific version only')
parser.add_argument(
    '--output-dir',
    dest='output_dir',
    help='Output directory for cache archives (default: current directory)',
    default='.',
)
parser.add_argument('--runtime-dir', dest='runtime_dir', help='Path to the BWC runtime directory', default=None)

args = parser.parse_args()

duckdb_root_dir = dirname(dirname(dirname(abspath(__file__))))


def get_runtime_dir():
    if args.runtime_dir:
        return args.runtime_dir
    return os.path.join(duckdb_root_dir, 'duckdb_unittest_tempdir', 'bwc', 'runtime')


def should_include(file_path, version):
    """Check if a file should be included in the cache archive."""
    basename = os.path.basename(file_path)
    # Include SQL query files
    if basename.endswith('.sql'):
        return True
    # Include serialized plans
    if basename.endswith('.plan.bin'):
        return True
    # Include old version results (e.g., test.v1.4.4.result.bin)
    if basename.endswith(f'.{version}.result.bin'):
        return True
    return False


def should_include_dir(dir_name):
    """Check if a directory should be included."""
    return dir_name == 'fixtures'


def export_version(version, runtime_dir, output_dir):
    version_dir = os.path.join(runtime_dir, version)
    if not os.path.isdir(version_dir):
        print(f"Warning: runtime directory not found for {version}: {version_dir}")
        return None

    archive_name = f"runtime_{version}.tar.gz"
    archive_path = os.path.join(output_dir, archive_name)

    file_count = 0
    with tarfile.open(archive_path, 'w:gz') as tar:
        for root, dirs, files in os.walk(version_dir):
            # Skip output directories and symlinks
            dirs[:] = [d for d in dirs if not d.startswith('output') and not os.path.islink(os.path.join(root, d))]

            rel_root = os.path.relpath(root, runtime_dir)

            # Include fixture directories entirely
            fixture_dirs = [d for d in dirs if should_include_dir(d)]
            for d in fixture_dirs:
                fixture_path = os.path.join(root, d)
                arcname = os.path.join(rel_root, d)
                tar.add(fixture_path, arcname=arcname)
                file_count += sum(len(f) for _, _, f in os.walk(fixture_path))
                # Don't recurse into fixtures again
                dirs.remove(d)

            for file in files:
                file_path = os.path.join(root, file)
                if should_include(file_path, version):
                    arcname = os.path.join(rel_root, file)
                    tar.add(file_path, arcname=arcname)
                    file_count += 1

    size_mb = os.path.getsize(archive_path) / (1024 * 1024)
    print(f"Exported {version}: {file_count} files -> {archive_path} ({size_mb:.1f} MB)")
    return archive_path


if __name__ == '__main__':
    runtime_dir = get_runtime_dir()
    if not os.path.isdir(runtime_dir):
        print(f"Error: runtime directory not found: {runtime_dir}")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    if args.version:
        versions = [args.version]
    else:
        versions = sorted(
            [d for d in os.listdir(runtime_dir) if d.startswith('v') and os.path.isdir(os.path.join(runtime_dir, d))]
        )

    if not versions:
        print(f"No version directories found in {runtime_dir}")
        sys.exit(1)

    print(f"Exporting cache for {len(versions)} version(s) from {runtime_dir}")
    exported = []
    for version in versions:
        result = export_version(version, runtime_dir, args.output_dir)
        if result:
            exported.append(result)

    print(f"\nDone: exported {len(exported)}/{len(versions)} archives to {args.output_dir}")
