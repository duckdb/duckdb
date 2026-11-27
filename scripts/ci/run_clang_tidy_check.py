#!/usr/bin/env python3

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


ERROR_PATTERN = re.compile(r"^[^\s].*: error:")
CLANG_TIDY_INVOCATION_PATTERN = re.compile(r"^(?:\S*/)?clang-tidy(?:-\d+)?(?:\s|$)")
CLANG_TIDY_STATUS_PATTERNS = (
    re.compile(r"^\d+ warnings?(?: and \d+ errors?)? generated\.$"),
    re.compile(r"^Suppressed \d+ warnings?.*$"),
    re.compile(r"^Error while processing .*$"),
    re.compile(r"^Found compiler error\(s\)\.$"),
)
BUILD_STATUS_PATTERN = re.compile(r"^(?:make(?:\[\d+\])?:|ninja:)")
WORKER_STATUS_PATTERN = re.compile(r"^Running clang-tidy shard ")
PROGRESS_PATTERN = re.compile(r"^Clang-tidy progress: ")
FAILURE_TAIL_LINES = 100


def is_error(line):
    return ERROR_PATTERN.search(line) is not None


def is_diagnostic_boundary(line):
    if (
        CLANG_TIDY_INVOCATION_PATTERN.search(line)
        or BUILD_STATUS_PATTERN.search(line)
        or WORKER_STATUS_PATTERN.search(line)
        or PROGRESS_PATTERN.search(line)
    ):
        return True
    return any(pattern.search(line) for pattern in CLANG_TIDY_STATUS_PATTERNS)


def unique_error_blocks(lines):
    blocks = {}
    current_error = None
    current_block = []

    def finish_block():
        if current_error is None:
            return
        while len(current_block) > 1 and not current_block[-1]:
            current_block.pop()
        if current_error not in blocks:
            blocks[current_error] = list(current_block)

    for line in lines:
        if is_error(line):
            finish_block()
            current_error = line
            current_block = [line]
        elif current_error is not None:
            if is_diagnostic_boundary(line):
                finish_block()
                current_error = None
                current_block = []
            else:
                current_block.append(line)

    finish_block()
    return [blocks[error] for error in sorted(blocks)]


def summarize(lines, command_exit_code):
    worker_status = [line for line in lines if WORKER_STATUS_PATTERN.search(line)]
    blocks = unique_error_blocks(lines)
    if blocks:
        label = "error" if len(blocks) == 1 else "errors"
        output = [*worker_status, f"Found {len(blocks)} unique clang-tidy {label}:", ""]
        for block_index, block in enumerate(blocks):
            if block_index:
                output.append("")
            output.extend(block)
        return output

    if command_exit_code:
        return [
            *worker_status,
            "Clang-tidy failed without a recognized diagnostic; showing the final "
            f"{min(FAILURE_TAIL_LINES, len(lines))} log lines:",
            "",
            *lines[-FAILURE_TAIL_LINES:],
        ]

    return [*worker_status, "No clang-tidy errors found."]


def main():
    parser = argparse.ArgumentParser(
        description="Run clang-tidy with live progress and a deduplicated diagnostic summary"
    )
    parser.add_argument("--log-file", type=Path, default=Path("clang-tidy.log"), help="complete clang-tidy log")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="command to run, preceded by '--'")
    args = parser.parse_args()
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        parser.error("missing command")

    env = dict(os.environ, PYTHONUNBUFFERED="1")
    with args.log_file.open("w", encoding="utf-8") as log_file:
        with subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        ) as process:
            for line in process.stdout:
                log_file.write(line)
                if WORKER_STATUS_PATTERN.search(line) or PROGRESS_PATTERN.search(line):
                    print(line, end="", flush=True)
            command_exit_code = process.wait()

    lines = args.log_file.read_text(encoding="utf-8", errors="replace").splitlines()
    print("\n".join(summarize(lines, command_exit_code)), flush=True)
    return command_exit_code


if __name__ == "__main__":
    sys.exit(main())
