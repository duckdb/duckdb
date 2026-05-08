#!/usr/bin/env python3
import argparse
import re
import subprocess
import sys

# Matches Ninja progress lines such as: [123/456] Building CXX object ...
NINJA_PROGRESS_RE = re.compile(r"^\[(\d+)/(\d+)\]\s+(.*)$")
BUILDING_OBJECT_RE = re.compile(r"^Building\s+.*\bobject\b")


def should_filter_ninja_message(message: str) -> bool:
    return BUILDING_OBJECT_RE.match(message) is not None


def filter_stream(stream) -> None:
    for raw_line in stream:
        line = raw_line.rstrip("\n")
        match = NINJA_PROGRESS_RE.match(line)
        if not match:
            print(line, flush=True)
            continue

        current = int(match.group(1))
        message = match.group(3)

        if should_filter_ninja_message(message) and current % 500 != 0:
            continue

        print(line, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Filter Ninja output, suppressing noisy compile lines.")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="Optional command to run and filter output for.")
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    return args


def main() -> int:
    args = parse_args()

    if not args.command:
        filter_stream(sys.stdin)
        return 0

    process = subprocess.Popen(
        args.command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1,
    )

    assert process.stdout is not None
    filter_stream(process.stdout)
    process.stdout.close()
    return process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
