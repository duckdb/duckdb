import argparse
import os
import math
import re

parser = argparse.ArgumentParser(description='Check code coverage results')

parser.add_argument(
    '--uncovered_files',
    action='store',
    help='Set of files that are not 100% covered',
    default=os.path.join(".github", "config", "uncovered_files.csv"),
)
parser.add_argument('--directory', help='Directory of generated HTML files', action='store', default='coverage_html')
parser.add_argument('--fix', help='Fill up the uncovered_files.csv with all files', action='store_true', default=False)


args = parser.parse_args()
if not os.path.exists(args.directory):
    print(f"The provided directory ({args.directory}) does not exist, please create it first")
    exit(1)

covered_regex = (
    r'<a name="(\d+)">[ \t\n]*<span class="lineNum">[ \t\n0-9]+</span><span class="{COVERED_CLASS}">[ \t\n0-9]+:([^<]+)'
)


def get_original_path(path):
    return (
        path.replace('.gcov.html', '')
        .replace(os.getcwd(), '')
        .replace('coverage_html' + os.path.sep, '')
        .replace('home/runner/work/duckdb/duckdb/', '')
    )


def cleanup_line(line):
    return line.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"')


partial_coverage_dict = {}
with open(args.uncovered_files, 'r') as f:
    for line in f.readlines():
        splits = line.split('\t')
        partial_coverage_dict[splits[0]] = int(splits[1].strip())

if args.fix:
    uncovered_file = open(args.uncovered_files, 'w+')

DASH_COUNT = 80
total_difference = 0
allowed_difference = 0


def check_file(path, partial_coverage_dict):
    global any_failed
    global total_difference
    if not '.cpp' in path and not '.hpp' in path:
        # files are named [path].[ch]pp
        return
    if not '.html' in path:
        return
    with open(path, 'r') as f:
        text = f.read()
    original_path = get_original_path(path)
    uncovered_lines = re.findall(covered_regex.replace('{COVERED_CLASS}', 'lineNoCov'), text)
    covered_lines = re.findall(covered_regex.replace('{COVERED_CLASS}', 'lineCov'), text)

    total_lines = len(uncovered_lines) + len(covered_lines)
    if total_lines == 0:
        # no lines to cover - skip
        return

    coverage_percentage = round(len(covered_lines) / (total_lines) * 100, 2)
    expected_uncovered_lines = 0
    if original_path in partial_coverage_dict:
        expected_uncovered_lines = partial_coverage_dict[original_path]
    if args.fix:
        if expected_uncovered_lines == 0 and len(uncovered_lines) == 0:
            return
        expected_uncovered = max(expected_uncovered_lines, len(uncovered_lines) + 1)
        uncovered_file.write(f'{original_path}\t{expected_uncovered}\n')
        return

    if len(uncovered_lines) > expected_uncovered_lines:
        total_difference += len(uncovered_lines) - expected_uncovered_lines

        print("-" * DASH_COUNT)
        print(f"Coverage failure in file {original_path}")
        print("-" * DASH_COUNT)
        print(f"Coverage percentage: {coverage_percentage}%")
        print(f"Uncovered lines: {len(uncovered_lines)}")
        print(f"Covered lines: {len(covered_lines)}")
        print("-" * DASH_COUNT)
        print(f"Expected uncovered lines: {expected_uncovered_lines}")
        print("-" * DASH_COUNT)
        print("Uncovered lines")
        print("-" * DASH_COUNT)
        for e in uncovered_lines:
            print(e[0] + ' ' * 8 + cleanup_line(e[1]))


def scan_directory(path):
    file_list = []
    if os.path.isfile(path):
        file_list.append(path)
    else:
        files = os.listdir(path)
        for file in files:
            file_list += scan_directory(os.path.join(path, file))
    return file_list


files = scan_directory(args.directory)
files.sort()

for file in files:
    check_file(file, partial_coverage_dict)

if args.fix:
    uncovered_file.close()

if total_difference > allowed_difference:
    exit(1)
elif total_difference > 0:
    print("-" * DASH_COUNT)
    print("SUCCESS-ish")
    print("-" * DASH_COUNT)
    print(f"{total_difference} lines were uncovered but this falls within the margin of {allowed_difference}")
