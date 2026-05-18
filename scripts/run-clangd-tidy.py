#!/usr/bin/env python3

import argparse
import json
import multiprocessing
import os
import shlex
import subprocess
import sys


def make_absolute(path, directory):
    if os.path.isabs(path):
        return os.path.normpath(path)
    return os.path.normpath(os.path.join(directory, path))


def is_ignored_file(path, repo_root):
    normalized = os.path.normpath(path)
    try:
        relative = os.path.relpath(normalized, repo_root)
    except ValueError:
        return False
    return relative == 'third_party' or relative.startswith('third_party' + os.sep)


def chunk_files(files, max_chars=100000):
    chunk = []
    current_chars = 0
    for path in files:
        path_len = len(path) + 1
        if chunk and current_chars + path_len > max_chars:
            yield chunk
            chunk = []
            current_chars = 0
        chunk.append(path)
        current_chars += path_len
    if chunk:
        yield chunk


def load_files(build_path):
    database_path = os.path.join(build_path, 'compile_commands.json')
    repo_root = os.path.abspath(os.path.join(build_path, '..', '..'))
    with open(database_path, 'r', encoding='utf-8') as handle:
        database = json.load(handle)

    seen = set()
    files = []
    for entry in database:
        path = make_absolute(entry['file'], entry['directory'])
        if path in seen or is_ignored_file(path, repo_root):
            continue
        seen.add(path)
        files.append(os.path.relpath(path, repo_root))
    return repo_root, files


def create_clangd_wrapper(build_path, clangd_binary):
    build_path = os.path.abspath(build_path)
    pch_dir = os.path.join(build_path, 'pchs')
    os.makedirs(pch_dir, exist_ok=True)

    wrapper_path = os.path.join(build_path, 'clangd-tidy-clangd-wrapper.sh')
    wrapper = f"""#!/bin/sh
set -eu
export TMPDIR={shlex.quote(pch_dir)}
export TMP={shlex.quote(pch_dir)}
export TEMP={shlex.quote(pch_dir)}
exec {shlex.quote(clangd_binary)} "$@" --pch-storage=disk
"""
    with open(wrapper_path, 'w', encoding='utf-8') as handle:
        handle.write(wrapper)
    os.chmod(wrapper_path, 0o755)
    return wrapper_path


def main():
    parser = argparse.ArgumentParser(description='Run clangd-tidy over all files in a compilation database.')
    parser.add_argument('-j', '--jobs', type=int, default=0, help='number of async workers passed to clangd-tidy')
    parser.add_argument('-p', '--build-path', default=None, help='path containing compile_commands.json')
    parser.add_argument(
        '--clangd-tidy-binary',
        default='clangd-tidy',
        help='path to the clangd-tidy executable',
    )
    parser.add_argument(
        '--clangd-binary',
        default='clangd',
        help='path to the clangd executable used by clangd-tidy',
    )
    parser.add_argument(
        '--query-driver',
        default=None,
        help='comma-separated list of query-driver globs passed to clangd-tidy',
    )
    args = parser.parse_args()

    build_path = os.path.abspath(args.build_path or os.getcwd())
    repo_root, files = load_files(build_path)
    if not files:
        print('No files found in compile_commands.json', file=sys.stderr)
        return 1

    jobs = args.jobs if args.jobs > 0 else multiprocessing.cpu_count()

    try:
        clangd_tidy_version = subprocess.check_output([args.clangd_tidy_binary, '--version'], text=True).splitlines()[0]
        clangd_version = subprocess.check_output([args.clangd_binary, '--version'], text=True).splitlines()[0]
    except Exception:
        print('Unable to run clangd-tidy or clangd.', file=sys.stderr)
        return 1
    print(f'Using clangd-tidy: {clangd_tidy_version}')
    print(f'Using clangd: {clangd_version}')

    clangd_wrapper = create_clangd_wrapper(build_path, args.clangd_binary)
    base_command = [
        args.clangd_tidy_binary,
        '--compile-commands-dir',
        build_path,
        '--jobs',
        str(jobs),
        '--clangd-executable',
        clangd_wrapper,
    ]
    if args.query_driver:
        base_command.extend(['--query-driver', args.query_driver])

    return_code = 0
    for chunk in chunk_files(files):
        result = subprocess.run(base_command + chunk, check=False, cwd=repo_root)
        if result.returncode != 0:
            return_code = result.returncode

    return return_code


if __name__ == '__main__':
    sys.exit(main())
