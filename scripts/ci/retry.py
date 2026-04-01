import argparse
import os
import subprocess
import sys
import time

RETRY_DELAY_SECONDS = 15.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retry a command when it exits with a non-zero status or times out.")
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Number of retries after the first attempt.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Optional per-attempt timeout in seconds.",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to execute. Prefix with '--' to separate wrapper flags from the command.",
    )
    args = parser.parse_args()
    if args.retries < 0:
        parser.error("--retries must be >= 0")
    if args.timeout is not None and args.timeout <= 0:
        parser.error("--timeout must be > 0")
    if not args.command:
        parser.error("missing command")
    if args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("missing command after '--'")
    return args


def format_command(command):
    return subprocess.list2cmdline(command) if os.name == "nt" else " ".join(command)


def run_command(command, command_text, timeout):
    if os.name == "nt":
        return subprocess.run(command_text, timeout=timeout, shell=True)
    return subprocess.run(command, timeout=timeout)


def main() -> int:
    args = parse_args()
    attempts = args.retries + 1
    command_text = format_command(args.command)

    for attempt in range(1, attempts + 1):
        try:
            completed = run_command(args.command, command_text, args.timeout)
            exit_code = completed.returncode
        except subprocess.TimeoutExpired:
            exit_code = 124
            print(
                f"[retry] attempt {attempt}/{attempts} timed out after {args.timeout} sec",
                flush=True,
            )
        except OSError as exc:
            exit_code = 127
            print(
                f"[retry] attempt {attempt}/{attempts} could not start command: {exc}",
                flush=True,
            )

        if exit_code == 0:
            return 0

        print(f"[retry] attempt {attempt}/{attempts} failed (exit code: {exit_code}) for: {command_text}", flush=True)
        if attempt == attempts:
            return exit_code

        print(f"[retry] sleeping for {RETRY_DELAY_SECONDS:g} seconds before retry", flush=True)
        time.sleep(RETRY_DELAY_SECONDS)

    return 1


if __name__ == "__main__":
    sys.exit(main())
