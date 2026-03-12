#!/usr/bin/env python3
"""
Updates the BWC test cache in the test-utils repository from CI artifacts.

This script downloads cache artifacts from a GitHub Actions workflow run
and copies them to the test-utils cache directory.

Usage:
  python3 test/bwc/update_cache.py --run-id 12345678 --test-utils-dir /path/to/test-utils [--version v1.4.4] [--commit]

The run ID can be found in the GitHub Actions URL for a SerializationBWC workflow run.
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

parser = argparse.ArgumentParser(description='Update BWC test cache in test-utils from CI artifacts')
parser.add_argument(
    '--run-id', dest='run_id', required=True, help='GitHub Actions run ID to download cache artifacts from'
)
parser.add_argument('--test-utils-dir', dest='test_utils_dir', required=True, help='Path to the test-utils repository')
parser.add_argument('--version', dest='version', help='Update cache for a specific version only')
parser.add_argument('--repo', dest='repo', default='duckdb/duckdb', help='GitHub repository (default: duckdb/duckdb)')
parser.add_argument('--commit', dest='commit', action='store_true', help='Commit the updated cache files in test-utils')

args = parser.parse_args()

SUPPORTED_VERSIONS = [
    "v1.1.0",
    "v1.1.1",
    "v1.1.2",
    "v1.1.3",
    "v1.2.0",
    "v1.2.1",
    "v1.2.2",
    "v1.3.0",
    "v1.3.1",
    "v1.3.2",
    "v1.4.0",
    "v1.4.1",
    "v1.4.2",
    "v1.4.3",
    "v1.4.4",
]


def download_artifact(run_id, artifact_name, dest_dir, repo):
    """Download a single artifact from a GitHub Actions run."""
    result = subprocess.run(
        ['gh', 'run', 'download', str(run_id), '--repo', repo, '--name', artifact_name, '--dir', dest_dir],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False, result.stderr.strip()
    return True, None


if __name__ == '__main__':
    cache_dir = os.path.join(args.test_utils_dir, 'cache')
    if not os.path.isdir(cache_dir):
        print(f"Error: cache directory not found in test-utils: {cache_dir}")
        sys.exit(1)

    if args.version:
        versions = [args.version]
    else:
        versions = SUPPORTED_VERSIONS

    print(f"Downloading cache artifacts from run {args.run_id} ({args.repo})")
    print(f"Updating {len(versions)} version(s) in {cache_dir}\n")

    updated = []
    failed = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for version in versions:
            artifact_name = f"bwc-cache-{version}"
            dl_dir = os.path.join(tmpdir, artifact_name)

            ok, err = download_artifact(args.run_id, artifact_name, dl_dir, args.repo)
            if not ok:
                print(f"  {version}: skipped ({err})")
                failed.append(version)
                continue

            archive_name = f"runtime_{version}.tar.gz"
            src = os.path.join(dl_dir, archive_name)
            if not os.path.isfile(src):
                print(f"  {version}: skipped (archive not found in artifact)")
                failed.append(version)
                continue

            dest = os.path.join(cache_dir, archive_name)
            shutil.copy2(src, dest)
            size_mb = os.path.getsize(dest) / (1024 * 1024)
            print(f"  {version}: updated ({size_mb:.1f} MB)")
            updated.append(version)

    print(f"\nUpdated {len(updated)}/{len(versions)} cache archives")
    if failed:
        print(f"Failed: {', '.join(failed)}")

    if not updated:
        print("No cache files were updated")
        sys.exit(1)

    if args.commit:
        print("\nCommitting changes to test-utils...")
        subprocess.run(['git', '-C', args.test_utils_dir, 'add', 'cache/'], check=True)
        version_str = args.version if args.version else f"{len(updated)} versions"
        subprocess.run(
            ['git', '-C', args.test_utils_dir, 'commit', '-m', f'Update BWC cache for {version_str}'], check=True
        )
        print("Committed. Run 'git push' in test-utils to publish.")
