#!/usr/bin/env python3

import argparse
import json
import multiprocessing
import os
import random
import shlex
import subprocess
import sys
import time


MAX_CHUNK_CHARS = 100000
MAX_RETRIES = 2
RETRY_BACKOFF_MS = 500
RETRY_PATTERNS = (
    'invalid header end',
    'broken pipe',
    'unexpected eof',
    'connection reset by peer',
    'failed to read message',
    'jsonrpc',
)


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


def chunk_files(files, max_chars=MAX_CHUNK_CHARS):
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


def create_attempt_clangd_wrapper(log_dir, clangd_binary, pch_dir, attempt_id):
    wrapper_path = os.path.join(log_dir, f'clangd-wrapper-attempt-{attempt_id}.sh')
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


def is_retryable_failure(result):
    if result.returncode == 0:
        return False
    text = (result.stdout or '') + '\n' + (result.stderr or '')
    lower = text.lower()
    return any(pattern in lower for pattern in RETRY_PATTERNS)


def write_attempt_logs(log_dir, attempt_id, chunk, result):
    with open(os.path.join(log_dir, f'attempt-{attempt_id}.stdout.log'), 'w', encoding='utf-8') as handle:
        handle.write(result.stdout or '')
    with open(os.path.join(log_dir, f'attempt-{attempt_id}.stderr.log'), 'w', encoding='utf-8') as handle:
        handle.write(result.stderr or '')
    with open(os.path.join(log_dir, f'attempt-{attempt_id}.files.txt'), 'w', encoding='utf-8') as handle:
        for entry in chunk:
            handle.write(entry + '\n')


def run_chunk_with_retries(base_command, chunk, repo_root, env, log_dir, pch_root, clangd_binary, attempt_counter):
    retries = 0
    while True:
        attempt_counter[0] += 1
        attempt_id = attempt_counter[0]
        attempt_pch_dir = os.path.join(pch_root, f'attempt-{attempt_id}')
        os.makedirs(attempt_pch_dir, exist_ok=True)
        clangd_wrapper = create_attempt_clangd_wrapper(log_dir, clangd_binary, attempt_pch_dir, attempt_id)
        command = base_command + ['--clangd-executable', clangd_wrapper] + chunk
        result = subprocess.run(command, check=False, cwd=repo_root, env=env, text=True, capture_output=True)
        write_attempt_logs(log_dir, attempt_id, chunk, result)
        if result.returncode == 0:
            return True
        if retries >= MAX_RETRIES or not is_retryable_failure(result):
            return False
        delay = (RETRY_BACKOFF_MS / 1000.0) * (2**retries) + random.uniform(0.0, 0.2)
        time.sleep(delay)
        retries += 1


def process_chunk(
    base_command, chunk, repo_root, env, log_dir, pch_root, clangd_binary, attempt_counter, hard_failures
):
    ok = run_chunk_with_retries(base_command, chunk, repo_root, env, log_dir, pch_root, clangd_binary, attempt_counter)
    if ok:
        return
    if len(chunk) == 1:
        hard_failures.append(chunk[0])
        return
    mid = len(chunk) // 2
    process_chunk(
        base_command, chunk[:mid], repo_root, env, log_dir, pch_root, clangd_binary, attempt_counter, hard_failures
    )
    process_chunk(
        base_command, chunk[mid:], repo_root, env, log_dir, pch_root, clangd_binary, attempt_counter, hard_failures
    )


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

    log_dir = os.path.join(build_path, 'clangd-tidy-logs')
    os.makedirs(log_dir, exist_ok=True)
    pch_root = os.path.join(build_path, 'pchs')
    os.makedirs(pch_root, exist_ok=True)
    base_command = [
        args.clangd_tidy_binary,
        '--compile-commands-dir',
        build_path,
        '--jobs',
        str(jobs),
    ]
    if args.query_driver:
        base_command.extend(['--query-driver', args.query_driver])

    run_env = os.environ.copy()
    run_env['LC_ALL'] = 'C'
    run_env['PYTHONUNBUFFERED'] = '1'

    attempt_counter = [0]
    hard_failures = []
    for chunk in chunk_files(files):
        process_chunk(
            base_command,
            chunk,
            repo_root,
            run_env,
            log_dir,
            pch_root,
            args.clangd_binary,
            attempt_counter,
            hard_failures,
        )

    if hard_failures:
        print('clangd-tidy hard failures after retries:', file=sys.stderr)
        for failed in hard_failures:
            print(f'  {failed}', file=sys.stderr)
    print(f'clangd-tidy logs written to: {log_dir}')

    return 1 if hard_failures else 0


if __name__ == '__main__':
    sys.exit(main())
