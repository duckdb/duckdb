#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def fail(message: str) -> None:
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a command in a write-restricted sandbox (macOS: sandbox-exec, Linux: bwrap)"
    )
    parser.add_argument(
        "--writable-dir",
        action="append",
        dest="writable_dirs",
        default=[],
        help="Directory that the sandboxed command may write to (repeatable)",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to run after --",
    )
    args = parser.parse_args()
    if not args.writable_dirs:
        fail("at least one --writable-dir is required")
    if not args.command:
        fail("missing command to run (use -- <command...>)")
    if args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        fail("missing command to run (use -- <command...>)")
    return args


def resolve_writable_dirs(raw_dirs: list[str]) -> list[Path]:
    writable_dirs: list[Path] = []
    seen: set[Path] = set()
    for raw in raw_dirs:
        if not raw:
            fail("empty writable directory")
        path = Path(raw)
        path.mkdir(parents=True, exist_ok=True)
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            writable_dirs.append(resolved)

    tmpdir = os.environ.get("TMPDIR")
    for candidate in [tmpdir, "/tmp", "/private/tmp"]:
        if not candidate:
            continue
        path = Path(candidate)
        if not path.exists():
            continue
        path.mkdir(parents=True, exist_ok=True)
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            writable_dirs.append(resolved)

    return writable_dirs


def run_darwin(command: list[str], writable_dirs: list[Path]) -> int:
    if shutil.which("sandbox-exec") is None:
        fail("sandbox-exec is required on macOS")

    profile_lines = [
        "(version 1)",
        "(deny default)",
        "(allow process*)",
        "(allow signal (target self))",
        "(allow sysctl-read)",
        "(allow mach-lookup)",
        "(allow file-read*)",
    ]
    for path in writable_dirs:
        escaped = str(path).replace("\\", "\\\\").replace('"', '\\"')
        profile_lines.append(f'(allow file-write* (subpath "{escaped}"))')

    profile_path: str | None = None
    tmp_root = os.environ.get("TMPDIR", "/tmp")
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", prefix="afl_sandbox.", suffix=".sb", dir=tmp_root, delete=False
        ) as profile:
            profile.write("\n".join(profile_lines))
            profile.write("\n")
            profile_path = profile.name
        proc = subprocess.run(["sandbox-exec", "-f", profile_path, "--", *command], check=False)
        return proc.returncode
    finally:
        if profile_path:
            try:
                os.unlink(profile_path)
            except FileNotFoundError:
                pass


def run_linux(command: list[str], writable_dirs: list[Path]) -> int:
    if shutil.which("bwrap") is None:
        fail("bwrap is required on Linux")

    args = [
        "bwrap",
        "--die-with-parent",
        "--new-session",
        "--ro-bind",
        "/",
        "/",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
    ]
    for path in writable_dirs:
        path_str = str(path)
        args.extend(["--bind", path_str, path_str])
    args.extend(["--", *command])

    proc = subprocess.run(args, check=False)
    return proc.returncode


def main() -> int:
    args = parse_args()
    writable_dirs = resolve_writable_dirs(args.writable_dirs)
    system = platform.system()
    if system == "Darwin":
        return run_darwin(args.command, writable_dirs)
    if system == "Linux":
        return run_linux(args.command, writable_dirs)
    fail(f"unsupported platform: {system}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
