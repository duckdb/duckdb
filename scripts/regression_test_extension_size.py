import os
import argparse
import subprocess
import tempfile
from pathlib import Path

# the threshold at which we consider something a regression (percentage)
regression_threshold_percentage = 0.20

parser = argparse.ArgumentParser(description='Generate TPC-DS reference results from Postgres.')
parser.add_argument(
    '--old', dest='old_extension_dir', action='store', help='Path to the old extension dir', required=True
)
parser.add_argument(
    '--new', dest='new_extension_dir', action='store', help='Path to the new extension dir', required=True
)
parser.add_argument(
    '--expect',
    dest='expected_extensions_raw',
    action='store',
    help='Comma separated list of expected extensions',
    required=True,
)

args = parser.parse_args()


expected_extensions = args.expected_extensions_raw.split(',')

exit_code = 0


def parse_extensions(dir):
    result = {}

    for root, dirs, files in os.walk(dir):
        for filename in files:
            if filename.endswith(".duckdb_extension"):
                result[Path(filename).stem] = os.path.join(root, filename)

    # Check all expected extensions are there
    for expected_extension in expected_extensions:
        if expected_extension not in result.keys():
            print(f"Did not find expected extension {expected_extension} in {dir}")
            exit(1)

    return result


old_extensions = parse_extensions(args.old_extension_dir)
new_extensions = parse_extensions(args.new_extension_dir)

matching_extensions = []

for extension in old_extensions.keys():
    if extension in new_extensions:
        matching_extensions.append(extension)

check_passed = True
error_message = ""

for extension in matching_extensions:
    old_size = os.path.getsize(old_extensions[extension])
    new_size = os.path.getsize(new_extensions[extension])

    print(f" - checking '{extension}': old size={old_size}, new_size={new_size}")

    if new_size / (old_size + 0.1) > (1.0 + regression_threshold_percentage):
        check_passed = False
        error_message += f" - Extension '{extension}' was bigger than expected {new_size}\n"
        error_message += f"   - old size: {old_size}\n"
        error_message += f"   - new size: {new_size}\n"

print()
if not check_passed:
    print("Extension size regression check failed:\n")
    print(error_message)
    exit(1)
else:
    print(f"All extensions passed the check!")
