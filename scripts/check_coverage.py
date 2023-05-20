import argparse
import os
import math
import re

parser = argparse.ArgumentParser(description='Check code coverage results')

parser.add_argument('--uncovered_files', action='store',
                    help='Set of files that are not 100% covered', default=os.path.join(".github", "config", "uncovered_files.csv"))
parser.add_argument('--directory', help='Directory of generated HTML files', action='store', default='coverage_html')
parser.add_argument('--fix', help='Fill up the uncovered_files.csv with all files', action='store_true', default=False)

args = parser.parse_args()

covered_regex = '<a name="(\d+)">[ \t\n]*<span class="lineNum">[ \t\n0-9]+</span><span class="{COVERED_CLASS}">[ \t\n0-9]+:([^<]+)'

def get_original_path(path):
    return path.replace('.gcov.html', '').replace(os.getcwd(), '').replace('coverage_html' + os.path.sep, '')

def cleanup_line(line):
    return line.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"')

partial_coverage_dict = {}
if args.fix:
    uncovered_file = open(args.uncovered_files, 'w+')
else:
    with open(args.uncovered_files, 'r') as f:
        for line in f.readlines():
            splits = line.split('\t')
            partial_coverage_dict[splits[0]] = [x.strip() for x in splits[1].split(',')]

any_failed = False
def check_file(path, partial_coverage_dict):
    global any_failed
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
    expected_uncovered_lines = []
    if original_path in partial_coverage_dict:
        expected_uncovered_lines = partial_coverage_dict[original_path]

    all_covered = True
    for line in uncovered_lines:
        if line[0] not in expected_uncovered_lines:
            all_covered = False


    if not all_covered:
        if args.fix:
            all_uncovered = ''
            for e in uncovered_lines:
                if len(all_uncovered) > 0:
                    all_uncovered += ','
                all_uncovered += e[0]
            uncovered_file.write(f'{original_path}\t{all_uncovered}\n')
            return
        DASH_COUNT = 80
        print("-" * DASH_COUNT)
        print(f"Coverage failure in file {original_path}")
        print("-" * DASH_COUNT)
        print(f"Coverage percentage: {coverage_percentage}%")
        print(f"Uncovered lines: {len(uncovered_lines)}")
        print(f"Covered lines: {len(covered_lines)}")
        if len(expected_uncovered_lines) > 0:
            print("-" * DASH_COUNT)
            print(f"Expected uncovered lines: {','.join(expected_uncovered_lines)}")
        print("-" * DASH_COUNT)
        print("Uncovered lines")
        print("-" * DASH_COUNT)
        for e in uncovered_lines:
            print(e[0] + ' ' * 8 + cleanup_line(e[1]))
        any_failed = True


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

if any_failed:
    exit(1)