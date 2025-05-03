#!/usr/bin/python3

import os
import subprocess
import sys
import argparse
import concurrent.futures
import difflib
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from format_test_benchmark import format_file_content


# Check formatter versions
def check_formatter_version(command, version_key, min_version, install_instruction):
    try:
        output = subprocess.check_output(command, text=True).strip()
        # Extract version number for clang-format (e.g., "11.0.1" from "clang-format version 11.0.1...")
        if command[0] == "clang-format":
            version = output.split()[2].split(".")[0]
        elif command[0] == "black":
            version = output.split()[1].split(".")[0]
        else:
            version = output.split()[0].split(".")[0]
        version_key_mismatch = version_key is not None and version_key not in output
        if version_key_mismatch or int(version) < min_version:
            print(f"Please install {install_instruction}")
            print(f"Required: {min_version}. Present: {version}")
            sys.exit(1)
    except Exception as e:
        print(f"Please install {install_instruction}: {e}")
        sys.exit(1)


# Configuration
EXTENSIONS = [
    ".cpp",
    ".ipp",
    ".c",
    ".hpp",
    ".h",
    ".cc",
    ".hh",
    "CMakeLists.txt",
    ".test",
    ".test_slow",
    ".test_coverage",
    ".benchmark",
    ".py",
    ".java",
]
FORMATTED_DIRECTORIES = [
    "src",
    "benchmark",
    "test",
    "tools",
    "examples",
    "extension",
    "scripts",
]
IGNORED_FILES = {
    "tpch_constants.hpp",
    "tpcds_constants.hpp",
    "_generated",
    "tpce_flat_input.hpp",
    "test_csv_header.hpp",
    "duckdb.cpp",
    "duckdb.hpp",
    "json.hpp",
    "sqlite3.h",
    "shell.c",
    "termcolor.hpp",
    "test_insert_invalid.test",
    "httplib.hpp",
    "os_win.c",
    "glob.c",
    "printf.c",
    "helper.hpp",
    "single_thread_ptr.hpp",
    "types.hpp",
    "default_views.cpp",
    "default_functions.cpp",
    "release.h",
    "genrand.cpp",
    "address.cpp",
    "visualizer_constants.hpp",
    "icu-collate.cpp",
    "icu-collate.hpp",
    "yyjson.cpp",
    "yyjson.hpp",
    "duckdb_pdqsort.hpp",
    "stubdata.cpp",
    "nf_calendar.cpp",
    "nf_calendar.h",
    "nf_localedata.cpp",
    "nf_localedata.h",
    "nf_zformat.cpp",
    "nf_zformat.h",
    "expr.cc",
    "function_list.cpp",
    "inlined_grammar.hpp",
}
IGNORED_DIRECTORIES = [
    ".eggs",
    "__pycache__",
    "dbgen",
    "tools/pythonpkg/duckdb",
    "tools/pythonpkg/build",
    "tools/rpkg/src/duckdb",
    "tools/rpkg/inst/include/cpp11",
    "extension/tpcds/dsdgen",
    "extension/jemalloc/jemalloc",
    "extension/icu/third_party",
    "tools/nodejs/src/duckdb",
]

CLANG_FORMAT = ["clang-format", "-i", "--sort-includes=0", "-style=file"]
BLACK_FORMAT = ["black", "-q", "--skip-string-normalization", "--line-length", "120"]
CMAKE_FORMAT = ["cmake-format", "-i"]
TEST_FORMAT = ["./scripts/format_test_benchmark.py", "-i"]

FORMAT_COMMANDS = {
    ".cpp": CLANG_FORMAT,
    ".ipp": CLANG_FORMAT,
    ".c": CLANG_FORMAT,
    ".hpp at": CLANG_FORMAT,
    ".hpp": CLANG_FORMAT,
    ".h": CLANG_FORMAT,
    ".hh": CLANG_FORMAT,
    ".cc": CLANG_FORMAT,
    ".txt": CMAKE_FORMAT,
    ".py": BLACK_FORMAT,
    ".java": CLANG_FORMAT,
    ".test": TEST_FORMAT,
    ".test_slow": TEST_FORMAT,
    ".test_coverage": TEST_FORMAT,
    ".benchmark": TEST_FORMAT,
}


# Argument parsing
def parse_args():
    parser = argparse.ArgumentParser(description="Format source code files")
    parser.add_argument(
        "revision",
        nargs="?",
        default="HEAD",
        help="Revision number to format all files (default: HEAD)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check formatting without modifying files (default)",
    )
    parser.add_argument("--all", action="store_true", help="Format all files")
    parser.add_argument("--fix", action="store_true", help="Fix formatting issues in files")
    parser.add_argument("--force", action="store_true", help="Format even non-standard files")
    parser.add_argument("--silent", action="store_true", help="Suppress output of formatted files")
    parser.add_argument("--noconfirm", action="store_true", help="Skip confirmation prompt for fixing")
    return parser.parse_args()


# File filtering
def file_is_ignored(full_path):
    path = Path(full_path)
    return path.name in IGNORED_FILES or any(ignored in str(path.parent) for ignored in IGNORED_DIRECTORIES)


def can_format_file(full_path):
    if not Path(full_path).is_file():
        return False
    if not any(full_path.endswith(ext) for ext in EXTENSIONS):
        return False
    if file_is_ignored(full_path):
        return False
    return any(full_path.startswith(d) for d in FORMATTED_DIRECTORIES)


# File collection
def get_changed_files(revision):
    result = subprocess.run(["git", "diff", "--name-only", revision], capture_output=True, text=True)
    return [f for f in result.stdout.splitlines() if can_format_file(f) and not file_is_ignored(f)]


def format_directory(directory):
    files = []
    for path in Path(directory).rglob("*"):
        if path.is_file() and can_format_file(str(path)):
            files.append(str(path))
    return files


def file_is_generated(text):
    if '// This file is automatically generated by scripts/' in text:
        return True
    return False


# Formatting logic
def format_file(full_path, check_only, force, silent):
    ext = Path(full_path).suffix
    if ext not in FORMAT_COMMANDS:
        if not force:
            print(f"File {full_path} is not normally formatted. Use --force to format.")
            sys.exit(1)
        return

    if not silent:
        print(full_path)

    # Capture original and formatted content for diff
    with open(full_path, "r", encoding="utf-8") as f:
        original = f.read()

    # do not format auto-generated files
    if file_is_generated(original) and ext != '.py':
        return

    if check_only:
        cmd = FORMAT_COMMANDS[ext]
        if cmd == TEST_FORMAT:
            # optimization: import and call the function directly
            # instead of running a subprocess
            with open(full_path, "r", encoding="utf-8") as f:
                original_lines = f.readlines()
            formatted, status = format_file_content(full_path, original_lines)
            if formatted is None:
                print(f"Failed to format {full_path}: {status}")
                sys.exit(1)
        else:
            # Remove -i flag for check mode to avoid modifying the file
            if "-i" in cmd:
                cmd = [arg for arg in cmd if arg != "-i"]
            cmd += "-"
            process = subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            formatted, stderr = process.communicate(input=original)
            if stderr:
                print(f"Failed to format {full_path}: {stderr}")
                sys.exit(1)
        if original != formatted:
            print(f"Differences found in {full_path}:")
            diff = difflib.unified_diff(
                original.splitlines(), formatted.splitlines(), fromfile=full_path, tofile=full_path
            )
            # Process diff lines: join first 2 with "", rest with "\n"
            diff_lines = list(diff)
            if len(diff_lines) < 2:
                diff_output = "".join(diff_lines)
            else:
                diff_output = "".join(diff_lines[:2]) + "\n".join(diff_lines[2:])
            print(diff_output)
            return full_path
    else:
        # Format in-place
        result = subprocess.run(FORMAT_COMMANDS[ext] + [full_path], capture_output=True, text=True)
        if result.stderr:
            print(f"Failed to format {full_path}: {result.stderr}")
            sys.exit(1)
    return None


def main():
    args = parse_args()
    check_only = args.check or not args.fix
    format_all = args.all

    check_formatter_version(["clang-format", "--version"], "11.", 11, "`pip install clang-format==11.0.1`")
    check_formatter_version(["black", "--version"], "black", 24, '`pip install "black>=24"`')
    check_formatter_version(["cmake-format", "--version"], None, 0, "`pip install cmake-format`")
    # Collect files to format
    files = []
    if Path(args.revision).is_file():
        print(f"{'Checking' if check_only else 'Formatting'} individual file: {args.revision}")
        files = [args.revision]
    elif Path(args.revision).is_dir():
        print(f"{'Checking' if check_only else 'Formatting'} files in directory: {args.revision}")
        files = [str(p) for p in Path(args.revision).iterdir() if p.is_file()]
    elif format_all:
        print(f"{'Checking' if check_only else 'Formatting'} all files")
        for d in FORMATTED_DIRECTORIES:
            files.extend(format_directory(d))
    else:
        if args.revision == "main":
            subprocess.run(["git", "fetch", "origin", "main:main"], check=True)
        print(f"{'Checking' if check_only else 'Formatting'} since branch or revision: {args.revision}")
        files = get_changed_files(args.revision)

    if not files:
        print("No files to format!")
        sys.exit(0)

    print("Files to process:")
    for f in files:
        print(f)

    # Confirm before fixing
    if not check_only and not args.noconfirm:
        if input("Continue with changes (y/n)? ").lower() != "y":
            print("Aborting.")
            sys.exit(0)

    # Process files in parallel
    difference_files = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        try:
            futures = [executor.submit(format_file, f, check_only, args.force, args.silent) for f in files]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    difference_files.append(result)
        except KeyboardInterrupt:
            executor.shutdown(wait=True, cancel_futures=True)
            raise

    # Report results
    if check_only and difference_files:
        print("\nFailed format-check: differences found in the following files:")
        for f in difference_files:
            print(f"- {f}")
        print('Run "python scripts/format.py --fix" to fix these differences.')
        sys.exit(1)
    elif check_only:
        print("Passed format-check")
    else:
        print("Formatting complete")


if __name__ == "__main__":
    main()
