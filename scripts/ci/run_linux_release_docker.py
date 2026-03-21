#!/usr/bin/env python3

import argparse
import os
import shlex
import subprocess
import sys

from linux_release_cli_matrix import image_for


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("libc", choices=["gnu", "musl"])
    parser.add_argument("command")
    parser.add_argument(
        "--run-args",
        default="",
        help="Extra arguments to pass to `docker run`, parsed with shell-style splitting.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolved docker command instead of executing it.",
    )
    return parser.parse_args()


def build_docker_command(libc: str, shell_command: str, docker_run_args: list[str]) -> list[str]:
    pwd = os.getcwd()
    docker_image = os.environ.get("LINUX_RELEASE_DOCKER_IMAGE", image_for(libc))

    docker_run_command = ["docker", "run", "--rm", *docker_run_args]
    workspace_args = ["-v", f"{pwd}:{pwd}", "-w", pwd]
    env_args = ["-e", "OVERRIDE_GIT_DESCRIBE", "-e", f"CCACHE_DIR={pwd}/.ccache"]
    container_command = [docker_image, "sh", "-lc", shell_command]

    return docker_run_command + workspace_args + env_args + container_command


def main() -> int:
    args = parse_args()
    docker_command = build_docker_command(args.libc, args.command, shlex.split(args.run_args))
    docker_command_str = " ".join(shlex.quote(part) for part in docker_command)

    print(docker_command_str)
    if not args.dry_run:
        subprocess.run(docker_command, check=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.returncode) from exc
