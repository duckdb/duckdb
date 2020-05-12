#!/usr/bin/python

# this script is used to format the source directory

import os
import time
import sys
import inspect
import subprocess

cpp_format_command = 'clang-format -i -sort-includes=${SORT_INCLUDES} -style=file "${FILE}"'
sql_format_command = 'pg_format "${FILE}" -o "${FILE}.out" && mv "${FILE}.out" "${FILE}"'
cmake_format_command = 'cmake-format -i "${FILE}"'
extensions = ['.cpp', '.c', '.hpp', '.h', '.cc', '.hh', '.sql', '.txt']
formatted_directories = ['src', 'benchmark', 'test', 'tools', 'examples']
ignored_files = ['tpch_constants.hpp', 'tpcds_constants.hpp', '_generated', 'tpce_flat_input.hpp',
                 'test_csv_header.hpp', 'duckdb.cpp', 'duckdb.hpp', 'json.hpp', 'sqlite3.h']
confirm = True
format_all = False

os.system("git fetch origin master:master")


def print_usage():
    print("Usage: python scripts/format.py [revision|--all] [--no-confirm]")
    print("   [revision]     is an optional revision number, all files that changed since that revision will be formatted (default=HEAD)")
    print(
        "                  if [revision] is set to --all, all files will be formatted")
    print("   [--no-confirm] if set, the confirm dialog is skipped")
    exit(1)


if len(sys.argv) == 1:
    revision = "HEAD"
elif len(sys.argv) >= 2:
    revision = sys.argv[1]
else:
    print_usage()

if len(sys.argv) > 2:
    for arg in sys.argv[2:]:
        if arg == '--no-confirm':
            confirm = False
        elif arg == '--confirm':
            confirm = True
        else:
            print_usage()

if revision == '--all':
    format_all = True


def can_format_file(full_path):
    global extensions, formatted_directories, ignored_files
    if not os.path.isfile(full_path):
        return False
    fname = full_path.split(os.path.sep)[-1]
    # check ignored files
    if fname in ignored_files:
        return False
    # skip files that end in .txt but are not CMakeLists.txt
    if full_path.endswith('.txt'):
        return fname == 'CMakeLists.txt'
    found = False
    # check file extension
    for ext in extensions:
        if full_path.endswith(ext):
            found = True
            break
    if not found:
        return False
    # now check file directory
    for dname in formatted_directories:
        if full_path.startswith(dname):
            return True
    return False


print("Formatting since branch or revision: " + revision)


def get_changed_files(revision):
    proc = subprocess.Popen(
        ['git', 'diff', '--name-only', revision], stdout=subprocess.PIPE)
    files = proc.stdout.read().decode('utf8').split('\n')
    changed_files = []
    for f in files:
        if not can_format_file(f):
            continue
        changed_files.append(f)
    return changed_files


if not format_all:
    changed_files = get_changed_files(revision)
    if len(changed_files) == 0:
        print("No changed files found!")
        exit(0)

    print("Changeset:")
    for fname in changed_files:
        print(fname)
else:
    print("Formatting all files")

if confirm:
    result = input("Continue with changes (y/n)?\n")
    if result != 'y':
        print("Aborting.")
        exit(0)

format_commands = {
    '.cpp': cpp_format_command,
    '.c': cpp_format_command,
    '.hpp': cpp_format_command,
    '.h': cpp_format_command,
    '.hh': cpp_format_command,
    '.cc': cpp_format_command,
    '.sql': sql_format_command,
    '.txt': cmake_format_command
}

header_top = "//===----------------------------------------------------------------------===//\n"
header_top += "//                         DuckDB\n" + "//\n"
header_bottom = "//\n" + "//\n"
header_bottom += "//===----------------------------------------------------------------------===//\n\n"
base_dir = os.path.join(os.getcwd(), 'src/include')


def format_file(f, full_path, directory, ext, sort_includes):
    if not os.path.isfile(full_path):
        return
    if f == 'list.hpp':
        # fill in list file
        list = [os.path.join(dp, f) for dp, dn, filenames in os.walk(
            directory) for f in filenames if os.path.splitext(f)[1] == '.hpp' and not f.endswith("list.hpp")]
        list = [x.replace('src/include/', '') for x in list]
        list.sort()
        with open(full_path, "w") as file:
            for x in list:
                file.write('#include "%s"\n' % (x))
    elif ext == ".hpp" and directory.startswith("src/include"):
        # format header in files
        header_middle = "// " + os.path.relpath(full_path, base_dir) + "\n"
        file = open(full_path, "r")
        lines = file.readlines()
        file.close()
        file = open(full_path, "w")
        file.write(header_top + header_middle + header_bottom)
        is_old_header = True
        for line in lines:
            if not (line.startswith("//") or line.startswith("\n")) and is_old_header:
                is_old_header = False
            if not is_old_header:
                file.write(line)
        file.close()
    elif ext == ".txt" and f != 'CMakeLists.txt':
        return
    format_command = format_commands[ext]
    cmd = format_command.replace("${FILE}", full_path).replace(
        "${SORT_INCLUDES}", "1" if sort_includes else "0")
    print(cmd)
    os.system(cmd)
    # remove empty lines at beginning and end of file
    with open(full_path, 'r') as f:
        text = f.read()
        text = text.strip() + "\n"
    with open(full_path, 'w+') as f:
        f.write(text)


def format_directory(directory, sort_includes=False):
    files = os.listdir(directory)
    for f in files:
        full_path = os.path.join(directory, f)
        if os.path.isdir(full_path):
            print(full_path)
            format_directory(full_path, sort_includes)
        elif can_format_file(full_path):
            format_file(f, full_path, directory, '.' +
                        f.split('.')[-1], sort_includes)


if format_all:
    os.system(cmake_format_command.replace("${FILE}", "CMakeLists.txt"))
    format_directory('src')
    format_directory('benchmark')
    format_directory('test')
    format_directory('tools')
    format_directory('examples')
else:
    for full_path in changed_files:
        splits = full_path.split(os.path.sep)
        fname = splits[-1]
        dirname = os.path.sep.join(splits[:-1])
        ext = '.' + full_path.split('.')[-1]
        format_file(fname, full_path, dirname, ext, False)
