#!/usr/bin/env python3
#
# ===- clang-tidy-diff.py - ClangTidy Diff Checker -----------*- python -*--===#
#
# Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
# See https://llvm.org/LICENSE.txt for license information.
# SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
#
# ===-----------------------------------------------------------------------===#

r"""
ClangTidy Diff Checker
======================
 
This script reads input from a unified diff, runs clang-tidy on all changed
files and outputs clang-tidy warnings in changed lines only. This is useful to
detect clang-tidy regressions in the lines touched by a specific patch.
Example usage for git/svn users:
 
  git diff -U0 HEAD^ | clang-tidy-diff.py -p1
  svn diff --diff-cmd=diff -x-U0 | \
      clang-tidy-diff.py -fix -checks=-*,modernize-use-override
 
"""

import argparse
import glob
import json
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import traceback

try:
    import yaml
except ImportError:
    yaml = None

is_py2 = sys.version[0] == "2"

if is_py2:
    import Queue as queue
else:
    import queue as queue


def run_tidy(task_queue, lock, timeout, failed_files):
    watchdog = None
    while True:
        command = task_queue.get()
        try:
            proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if timeout is not None:
                watchdog = threading.Timer(timeout, proc.kill)
                watchdog.start()

            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                if proc.returncode < 0:
                    msg = "Terminated by signal %d : %s\n" % (
                        -proc.returncode,
                        " ".join(command),
                    )
                    stderr += msg.encode("utf-8")
                failed_files.append(command)

            with lock:
                sys.stdout.write(stdout.decode("utf-8") + "\n")
                sys.stdout.flush()
                if stderr:
                    sys.stderr.write(stderr.decode("utf-8") + "\n")
                    sys.stderr.flush()
        except Exception as e:
            with lock:
                sys.stderr.write("Failed: " + str(e) + ": ".join(command) + "\n")
        finally:
            with lock:
                if not (timeout is None or watchdog is None):
                    if not watchdog.is_alive():
                        sys.stderr.write("Terminated by timeout: " + " ".join(command) + "\n")
                    watchdog.cancel()
            task_queue.task_done()


def start_workers(max_tasks, tidy_caller, arguments):
    for _ in range(max_tasks):
        t = threading.Thread(target=tidy_caller, args=arguments)
        t.daemon = True
        t.start()


def merge_replacement_files(tmpdir, mergefile):
    """Merge all replacement files in a directory into a single file"""
    # The fixes suggested by clang-tidy >= 4.0.0 are given under
    # the top level key 'Diagnostics' in the output yaml files
    mergekey = "Diagnostics"
    merged = []
    for replacefile in glob.iglob(os.path.join(tmpdir, "*.yaml")):
        content = yaml.safe_load(open(replacefile, "r"))
        if not content:
            continue  # Skip empty files.
        merged.extend(content.get(mergekey, []))

    if merged:
        # MainSourceFile: The key is required by the definition inside
        # include/clang/Tooling/ReplacementsYaml.h, but the value
        # is actually never used inside clang-apply-replacements,
        # so we set it to '' here.
        output = {"MainSourceFile": "", mergekey: merged}
        with open(mergefile, "w") as out:
            yaml.safe_dump(output, out)
    else:
        # Empty the file:
        open(mergefile, "w").close()


def main():
    parser = argparse.ArgumentParser(
        description="Run clang-tidy against changed files, and " "output diagnostics only for modified " "lines."
    )
    parser.add_argument(
        "-clang-tidy-binary",
        metavar="PATH",
        default="clang-tidy",
        help="path to clang-tidy binary",
    )
    parser.add_argument(
        "-p",
        metavar="NUM",
        default=0,
        help="strip the smallest prefix containing P slashes",
    )
    parser.add_argument(
        "-regex",
        metavar="PATTERN",
        default=None,
        help="custom pattern selecting file paths to check " "(case sensitive, overrides -iregex)",
    )
    parser.add_argument(
        "-iregex",
        metavar="PATTERN",
        default=r".*\.(cpp|cc|c\+\+|cxx|c|cl|h|hpp|m|mm|inc)",
        help="custom pattern selecting file paths to check " "(case insensitive, overridden by -regex)",
    )
    parser.add_argument(
        "-j",
        type=int,
        default=1,
        help="number of tidy instances to be run in parallel.",
    )
    parser.add_argument("-timeout", type=int, default=None, help="timeout per each file in seconds.")
    parser.add_argument("-fix", action="store_true", default=False, help="apply suggested fixes")
    parser.add_argument(
        "-checks",
        help="checks filter, when not specified, use clang-tidy " "default",
        default="",
    )
    parser.add_argument(
        "-config-file",
        dest="config_file",
        help="Specify the path of .clang-tidy or custom config file",
        default="",
    )
    parser.add_argument("-use-color", action="store_true", help="Use colors in output")
    parser.add_argument("-path", dest="build_path", help="Path used to read a compile command database.")
    if yaml:
        parser.add_argument(
            "-export-fixes",
            metavar="FILE_OR_DIRECTORY",
            dest="export_fixes",
            help="A directory or a yaml file to store suggested fixes in, "
            "which can be applied with clang-apply-replacements. If the "
            "parameter is a directory, the fixes of each compilation unit are "
            "stored in individual yaml files in the directory.",
        )
    else:
        parser.add_argument(
            "-export-fixes",
            metavar="DIRECTORY",
            dest="export_fixes",
            help="A directory to store suggested fixes in, which can be applied "
            "with clang-apply-replacements. The fixes of each compilation unit are "
            "stored in individual yaml files in the directory.",
        )
    parser.add_argument(
        "-extra-arg",
        dest="extra_arg",
        action="append",
        default=[],
        help="Additional argument to append to the compiler " "command line.",
    )
    parser.add_argument(
        "-extra-arg-before",
        dest="extra_arg_before",
        action="append",
        default=[],
        help="Additional argument to prepend to the compiler " "command line.",
    )
    parser.add_argument(
        "-quiet",
        action="store_true",
        default=False,
        help="Run clang-tidy in quiet mode",
    )
    parser.add_argument(
        "-load",
        dest="plugins",
        action="append",
        default=[],
        help="Load the specified plugin in clang-tidy.",
    )
    parser.add_argument(
        "-allow-no-checks",
        action="store_true",
        help="Allow empty enabled checks.",
    )

    clang_tidy_args = []
    argv = sys.argv[1:]
    if "--" in argv:
        clang_tidy_args.extend(argv[argv.index("--") :])
        argv = argv[: argv.index("--")]

    args = parser.parse_args(argv)

    # Extract changed lines for each file.
    filename = None
    lines_by_file = {}
    for line in sys.stdin:
        match = re.search('^\\+\\+\\+\\ "?(.*?/){%s}([^ \t\n"]*)' % args.p, line)
        if match:
            filename = match.group(2)
        if filename is None:
            continue

        if args.regex is not None:
            if not re.match("^%s$" % args.regex, filename):
                continue
        else:
            if not re.match("^%s$" % args.iregex, filename, re.IGNORECASE):
                continue

        match = re.search(r"^@@.*\+(\d+)(,(\d+))?", line)
        if match:
            start_line = int(match.group(1))
            line_count = 1
            if match.group(3):
                line_count = int(match.group(3))
            if line_count == 0:
                continue
            end_line = start_line + line_count - 1
            lines_by_file.setdefault(filename, []).append([start_line, end_line])

    if not any(lines_by_file):
        print("No relevant changes found.")
        sys.exit(0)

    max_task_count = args.j
    if max_task_count == 0:
        max_task_count = multiprocessing.cpu_count()
    max_task_count = min(len(lines_by_file), max_task_count)

    combine_fixes = False
    export_fixes_dir = None
    delete_fixes_dir = False
    if args.export_fixes is not None:
        # if a directory is given, create it if it does not exist
        if args.export_fixes.endswith(os.path.sep) and not os.path.isdir(args.export_fixes):
            os.makedirs(args.export_fixes)

        if not os.path.isdir(args.export_fixes):
            if not yaml:
                raise RuntimeError(
                    "Cannot combine fixes in one yaml file. Either install PyYAML or specify an output directory."
                )

            combine_fixes = True

        if os.path.isdir(args.export_fixes):
            export_fixes_dir = args.export_fixes

    if combine_fixes:
        export_fixes_dir = tempfile.mkdtemp()
        delete_fixes_dir = True

    # Tasks for clang-tidy.
    task_queue = queue.Queue(max_task_count)
    # A lock for console output.
    lock = threading.Lock()

    # List of files with a non-zero return code.
    failed_files = []

    # Run a pool of clang-tidy workers.
    start_workers(max_task_count, run_tidy, (task_queue, lock, args.timeout, failed_files))

    # Form the common args list.
    common_clang_tidy_args = []
    if args.fix:
        common_clang_tidy_args.append("-fix")
    if args.checks != "":
        common_clang_tidy_args.append("-checks=" + args.checks)
    if args.config_file != "":
        common_clang_tidy_args.append("-config-file=" + args.config_file)
    if args.quiet:
        common_clang_tidy_args.append("-quiet")
    if args.build_path is not None:
        common_clang_tidy_args.append("-p=%s" % args.build_path)
    if args.use_color:
        common_clang_tidy_args.append("--use-color")
    if args.allow_no_checks:
        common_clang_tidy_args.append("--allow-no-checks")
    for arg in args.extra_arg:
        common_clang_tidy_args.append("-extra-arg=%s" % arg)
    for arg in args.extra_arg_before:
        common_clang_tidy_args.append("-extra-arg-before=%s" % arg)
    for plugin in args.plugins:
        common_clang_tidy_args.append("-load=%s" % plugin)

    for name in lines_by_file:
        line_filter_json = json.dumps([{"name": name, "lines": lines_by_file[name]}], separators=(",", ":"))

        # Run clang-tidy on files containing changes.
        command = [args.clang_tidy_binary]
        command.append("-line-filter=" + line_filter_json)
        if args.export_fixes is not None:
            # Get a temporary file. We immediately close the handle so clang-tidy can
            # overwrite it.
            (handle, tmp_name) = tempfile.mkstemp(suffix=".yaml", dir=export_fixes_dir)
            os.close(handle)
            command.append("-export-fixes=" + tmp_name)
        command.extend(common_clang_tidy_args)
        command.append(name)
        command.extend(clang_tidy_args)

        task_queue.put(command)

    # Application return code
    return_code = 0

    # Wait for all threads to be done.
    task_queue.join()
    # Application return code
    return_code = 0
    if failed_files:
        return_code = 1

    if combine_fixes:
        print("Writing fixes to " + args.export_fixes + " ...")
        try:
            merge_replacement_files(export_fixes_dir, args.export_fixes)
        except:
            sys.stderr.write("Error exporting fixes.\n")
            traceback.print_exc()
            return_code = 1

    if delete_fixes_dir:
        shutil.rmtree(export_fixes_dir)
    sys.exit(return_code)


if __name__ == "__main__":
    main()
