import argparse
import os
import re
import signal
import subprocess
import sys
import time

RETRY_DELAY_SECONDS = 15.0
TERMINATE_GRACE_SECONDS = 10.0
PROCESS_GROUP_POLL_SECONDS = 0.1


def parse_timeout(timeout: str) -> float:
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)([smhSMH]?)\s*", timeout)
    if not match:
        raise ValueError("invalid timeout format")
    value = float(match.group(1))
    unit = match.group(2).lower()
    if unit == "m":
        value *= 60.0
    elif unit == "h":
        value *= 3600.0
    if value <= 0:
        raise ValueError("timeout must be > 0")
    return value


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
        type=str,
        default=None,
        help="Optional per-attempt timeout (e.g. '30', '45s', '2m', '1.5h').",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to execute. Prefix with '--' to separate wrapper flags from the command.",
    )
    args = parser.parse_args()
    if args.retries < 0:
        parser.error("--retries must be >= 0")
    if args.timeout is not None:
        try:
            args.timeout_seconds = parse_timeout(args.timeout)
        except ValueError:
            parser.error("--timeout must be a positive duration (e.g. '30', '45s', '2m', '1.5h')")
    else:
        args.timeout_seconds = None
    if not args.command:
        parser.error("missing command")
    if args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("missing command after '--'")
    return args


def format_command(command):
    return subprocess.list2cmdline(command) if os.name == "nt" else " ".join(command)


def process_group_exists(process_group_id):
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def wait_for_process_group_exit(process, process_group_id):
    deadline = time.monotonic() + TERMINATE_GRACE_SECONDS
    while process_group_exists(process_group_id):
        process.poll()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(PROCESS_GROUP_POLL_SECONDS, remaining))
    return True


def terminate_posix_process_group(process):
    process_group_id = process.pid
    try:
        os.killpg(process_group_id, signal.SIGTERM)
    except ProcessLookupError:
        process.wait()
        return

    if not wait_for_process_group_exit(process, process_group_id):
        try:
            os.killpg(process_group_id, signal.SIGKILL)
        except (PermissionError, ProcessLookupError):
            pass
        wait_for_process_group_exit(process, process_group_id)
    process.wait()


def terminate_windows_process_tree(process):
    try:
        completed = subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        completed = None
    if completed is None or (completed.returncode != 0 and process.poll() is None):
        process.kill()
    process.wait()


def terminate_process_tree(process):
    if os.name == "nt":
        terminate_windows_process_tree(process)
    else:
        terminate_posix_process_group(process)


def run_command(command, command_text, timeout):
    if timeout is None:
        if os.name == "nt":
            return subprocess.run(command_text, shell=True)
        return subprocess.run(command)

    if os.name == "nt":
        process = subprocess.Popen(
            command_text,
            shell=True,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
        )
    else:
        process = subprocess.Popen(command, start_new_session=True)

    try:
        return_code = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        terminate_process_tree(process)
        raise
    return subprocess.CompletedProcess(command, return_code)


def main() -> int:
    args = parse_args()
    attempts = args.retries + 1
    command_text = format_command(args.command)

    for attempt in range(1, attempts + 1):
        try:
            completed = run_command(args.command, command_text, args.timeout_seconds)
            exit_code = completed.returncode
        except subprocess.TimeoutExpired:
            exit_code = 124
            print(
                f"[retry] attempt {attempt}/{attempts} timed out after {args.timeout}",
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
