import argparse
import sys
import subprocess
import re
import os

DEFAULT_UNITTEST_PATH = 'build/release/test/unittest'

parser = argparse.ArgumentParser(description='Print a list of tests to run.')
parser.add_argument(
    '--file-contains',
    dest='file_contains',
    action='store',
    help='Filter based on a string contained in the text',
    default=None,
)
parser.add_argument(
    '--unittest',
    dest='unittest',
    action='store',
    help='The path to the unittest program',
    default=DEFAULT_UNITTEST_PATH,
)
parser.add_argument('--list', dest='filter', action='store', help='The unittest filter to apply', default='')

args = parser.parse_args()

file_contains = args.file_contains
extra_args = [args.filter]
unittest_program = args.unittest

# Override default for windows
if os.name == 'nt' and unittest_program == DEFAULT_UNITTEST_PATH:
    unittest_program = 'build/release/test/Release/unittest.exe'

proc = subprocess.Popen([unittest_program, '-l'] + extra_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout = proc.stdout.read().decode('utf8')
stderr = proc.stderr.read().decode('utf8')
if proc.returncode is not None and proc.returncode != 0:
    print("Failed to run program " + unittest_program)
    print(proc.returncode)
    print(stdout)
    print(stderr)
    exit(1)

test_cases = []
for line in stdout.splitlines()[1:]:
    if not line.strip():
        continue
    splits = line.rsplit('\t', 1)
    if file_contains is not None:
        if not os.path.isfile(splits[0]):
            continue
        try:
            with open(splits[0], 'r') as f:
                text = f.read()
        except UnicodeDecodeError:
            continue
        if file_contains not in text:
            continue
    print(splits[0])
