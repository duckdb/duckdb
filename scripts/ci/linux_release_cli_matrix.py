#!/usr/bin/env python3

import argparse
import json
import os
import platform
import sys

GNU_AMD64_IMAGE = "quay.io/pypa/manylinux_2_28_x86_64"
GNU_ARM64_IMAGE = "quay.io/pypa/manylinux_2_28_aarch64"
MUSL_IMAGE = "alpine:3.22"


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def current_arch() -> str:
    machine = platform.machine().lower()
    if machine in {"x86_64", "amd64"}:
        return "amd64"
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    raise ValueError(f"unsupported platform architecture: {machine}")


def image_for(libc: str, arch: str | None = None) -> str:
    arch = arch or current_arch()
    if libc == "musl":
        return MUSL_IMAGE
    if libc == "gnu" and arch == "amd64":
        return GNU_AMD64_IMAGE
    if libc == "gnu" and arch == "arm64":
        return GNU_ARM64_IMAGE
    raise ValueError("unsupported libc/arch combination")


def build_matrix(kind: str) -> dict[str, list[dict[str, str]]]:
    x64_runner = env("LINUX_RELEASE_RUNNER_X64", "ubuntu-latest")
    arm64_runner = env("LINUX_RELEASE_RUNNER_ARM64", "ubuntu-24.04-arm")

    if kind not in {"pull_request", "default"}:
        raise ValueError("kind must be 'pull_request' or 'default'")

    include = [
        {
            "runner": x64_runner,
            "arch": "amd64",
            "libc": "gnu",
            "container_image": GNU_AMD64_IMAGE,
        },
        {
            "runner": arm64_runner,
            "arch": "arm64",
            "libc": "musl",
            "container_image": MUSL_IMAGE,
        },
    ]

    if kind != "pull_request":
        include.extend(
            [
                {
                    "runner": arm64_runner,
                    "arch": "arm64",
                    "libc": "gnu",
                    "container_image": GNU_ARM64_IMAGE,
                },
                {
                    "runner": x64_runner,
                    "arch": "amd64",
                    "libc": "musl",
                    "container_image": MUSL_IMAGE,
                },
            ]
        )

    return {"include": include}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        usage=(
            "linux_release_cli_matrix.py <pull_request|default>\n"
            "       linux_release_cli_matrix.py image <gnu|musl> [amd64|arm64]"
        )
    )
    parser.add_argument("command", choices=["pull_request", "default", "image"])
    parser.add_argument("libc", nargs="?")
    parser.add_argument("arch", nargs="?")
    args = parser.parse_args()

    if args.command == "image":
        if args.libc is None:
            parser.error("image requires <gnu|musl>")
        if args.libc not in {"gnu", "musl"}:
            parser.error("image libc must be one of: gnu, musl")
        if args.arch is not None and args.arch not in {"amd64", "arm64"}:
            parser.error("image arch must be one of: amd64, arm64")
    elif args.libc is not None or args.arch is not None:
        parser.error(f"{args.command} does not take additional arguments")

    return args


def main() -> int:
    args = parse_args()

    if args.command == "image":
        try:
            print(image_for(args.libc, args.arch))
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        return 0

    try:
        matrix = build_matrix(args.command)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(matrix, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
