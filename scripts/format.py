#!/usr/bin/python

# this script is used to format the source directory

import os
import time
import sys
import inspect
import subprocess
import difflib
from python_helpers import open_utf8

cpp_format_command = 'clang-format --sort-includes=0 -style=file'
cmake_format_command = 'cmake-format'
extensions = ['.cpp', '.c', '.hpp', '.h', '.cc', '.hh', 'CMakeLists.txt', '.test', '.test_slow']
formatted_directories = ['src', 'benchmark', 'test', 'tools', 'examples', 'extension']
ignored_files = ['tpch_constants.hpp', 'tpcds_constants.hpp', '_generated', 'tpce_flat_input.hpp',
                 'test_csv_header.hpp', 'duckdb.cpp', 'duckdb.hpp', 'json.hpp', 'sqlite3.h', 'shell.c',
                 'termcolor.hpp', 'test_insert_invalid.test', 'httplib.hpp', 'os_win.c', 'glob.c', 'printf.c',
                 'helper.hpp', 'single_thread_ptr.hpp','types.hpp', 'default_views.cpp', 'default_functions.cpp',
                 'release.h', 'genrand.cpp', 'address.cpp']
ignored_directories = ['.eggs', '__pycache__', 'icu', 'dbgen', os.path.join('tools', 'pythonpkg', 'duckdb'), os.path.join('tools', 'pythonpkg', 'build'), os.path.join('tools', 'rpkg', 'src', 'duckdb')]
format_all = False
check_only = True
confirm = True
silent = False

def print_usage():
    print("Usage: python scripts/format.py [revision|--all] [--check|--fix]")
    print("   [revision]     is an optional revision number, all files that changed since that revision will be formatted (default=HEAD)")
    print(
        "                  if [revision] is set to --all, all files will be formatted")
    print("   --check only prints differences, --fix also fixes the files (--check is default)")
    exit(1)


if len(sys.argv) == 1:
    revision = "HEAD"
elif len(sys.argv) >= 2:
    revision = sys.argv[1]
else:
    print_usage()

if len(sys.argv) > 2:
    for arg in sys.argv[2:]:
        if arg == '--check':
            check_only = True
        elif arg == '--fix':
            check_only = False
        elif arg == '--noconfirm':
            confirm = False
        elif arg == '--confirm':
            confirm = True
        elif arg == '--silent':
            silent = True
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


action = "Formatting"
if check_only:
    action = "Checking"

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

if os.path.isfile(revision):
    print(action + " individual file: " + revision)
    changed_files = [revision]
elif os.path.isdir(revision):
    print(action + " files in directory: " + revision)
    changed_files = [os.path.join(revision, x) for x in os.listdir(revision)]

    print("Changeset:")
    for fname in changed_files:
        print(fname)
elif not format_all:
    if revision == 'master':
        # fetch new changes when comparing to the master
        os.system("git fetch origin master:master")
    print(action + " since branch or revision: " + revision)
    changed_files = get_changed_files(revision)
    if len(changed_files) == 0:
        print("No changed files found!")
        exit(0)

    print("Changeset:")
    for fname in changed_files:
        print(fname)
else:
    print(action + " all files")

if confirm and not check_only:
    print("The files listed above will be reformatted.")
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
    '.txt': cmake_format_command
}

difference_files = []

header_top = "//===----------------------------------------------------------------------===//\n"
header_top += "//                         DuckDB\n" + "//\n"
header_bottom = "//\n" + "//\n"
header_bottom += "//===----------------------------------------------------------------------===//\n\n"
base_dir = os.path.join(os.getcwd(), 'src/include')

def get_formatted_text(f, full_path, directory, ext):
    if not can_format_file(full_path):
        print("Eek, cannot format file " + full_path + " but attempted to format anyway")
        exit(1)
    if f == 'list.hpp':
        # fill in list file
        file_list = [os.path.join(dp, f) for dp, dn, filenames in os.walk(
            directory) for f in filenames if os.path.splitext(f)[1] == '.hpp' and not f.endswith("list.hpp")]
        file_list = [x.replace('src/include/', '') for x in file_list]
        file_list.sort()
        result = ""
        for x in file_list:
            result += '#include "%s"\n' % (x)
        return result

    if ext == ".hpp" and directory.startswith("src/include"):
        f = open_utf8(full_path, 'r')
        lines = f.readlines()
        f.close()

        # format header in files
        header_middle = "// " + os.path.relpath(full_path, base_dir) + "\n"
        text = header_top + header_middle + header_bottom
        is_old_header = True
        for line in lines:
            if not (line.startswith("//") or line.startswith("\n")) and is_old_header:
                is_old_header = False
            if not is_old_header:
                text += line

    if ext == '.test' or ext == '.test_slow':
        f = open_utf8(full_path, 'r')
        lines = f.readlines()
        f.close()

        found_name = False
        found_group = False
        group_name = full_path.split('/')[-2]
        new_path_line = '# name: ' + full_path + '\n'
        new_group_line =  '# group: [' + group_name + ']' + '\n'
        found_diff = False
        for i in range(0, len(lines)):
            line = lines[i]
            if line.startswith('# name: ') or line.startswith('#name: '):
                if found_name:
                    print("Error formatting file " + full_path + ", multiple lines starting with # name found")
                    exit(1)
                found_name = True
                if lines[i] != new_path_line:
                    lines[i] = new_path_line
            if line.startswith('# group: ') or line.startswith('#group: '):
                if found_group:
                    print("Error formatting file " + full_path + ", multiple lines starting with # group found")
                    exit(1)
                found_group = True
                if lines[i] != new_group_line:
                    lines[i] = new_group_line
        if not found_group:
            lines = [new_group_line] + lines
        if not found_name:
            lines = [new_path_line] + lines
        return ''.join(lines)
    proc_command = format_commands[ext].split(' ') + [full_path]
    proc = subprocess.Popen(proc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    new_text = proc.stdout.read().decode('utf8')
    stderr = proc.stderr.read().decode('utf8')
    if len(stderr) > 0:
        print(os.getcwd())
        print("Failed to format file " + full_path)
        print(' '.join(proc_command))
        print(stderr)
        exit(1)
    return new_text

def format_file(f, full_path, directory, ext):
    global difference_files
    f = open_utf8(full_path, 'r')
    old_lines = f.read().split('\n')
    f.close()

    new_text = get_formatted_text(f, full_path, directory, ext)
    if check_only:
        new_lines = new_text.split('\n')
        old_lines = [x for x in old_lines if '...' not in x]
        new_lines = [x for x in new_lines if '...' not in x]
        diff_result = difflib.unified_diff(old_lines, new_lines)
        total_diff = ""
        for diff_line in diff_result:
            total_diff += diff_line + "\n"
        total_diff = total_diff.strip()
        if len(total_diff) > 0:
            print("----------------------------------------")
            print("----------------------------------------")
            print("Found differences in file " + full_path)
            print("----------------------------------------")
            print("----------------------------------------")
            print(total_diff)
            difference_files.append(full_path)
    else:
        tmpfile = full_path + ".tmp"
        f = open_utf8(tmpfile, 'w+')
        f.write(new_text)
        f.close()
        os.rename(tmpfile, full_path)

def format_directory(directory):
    files = os.listdir(directory)
    files.sort()
    for f in files:
        full_path = os.path.join(directory, f)
        if os.path.isdir(full_path):
            if f in ignored_directories or full_path in ignored_directories:
                continue
            if not silent:
                print(full_path)
            format_directory(full_path)
        elif can_format_file(full_path):
            format_file(f, full_path, directory, '.' +
                        f.split('.')[-1])


if format_all:
    try:
        os.system(cmake_format_command.replace("${FILE}", "CMakeLists.txt"))
    except:
        pass
    format_directory('src')
    format_directory('benchmark')
    format_directory('test')
    format_directory('tools')
    format_directory('examples')
    format_directory('extension')

else:
    for full_path in changed_files:
        splits = full_path.split(os.path.sep)
        fname = splits[-1]
        dirname = os.path.sep.join(splits[:-1])
        ext = '.' + full_path.split('.')[-1]
        format_file(fname, full_path, dirname, ext)

if check_only:
    if len(difference_files) > 0:
        print("")
        print("")
        print("")
        print("Failed format-check: differences were found in the following files:")
        for fname in difference_files:
            print("- " + fname)
        print('Run "make format-fix" to fix these differences automatically')
        exit(1)
    else:
        print("Passed format-check")
        exit(0)
