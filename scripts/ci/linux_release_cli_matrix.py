#!/usr/bin/env python3

import json
import os
import sys

GNU_AMD64_IMAGE = "quay.io/pypa/manylinux_2_28_x86_64"
GNU_ARM64_IMAGE = "quay.io/pypa/manylinux_2_28_aarch64"
MUSL_IMAGE = "alpine:3.22"


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


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


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: linux_release_cli_matrix.py <pull_request|default>", file=sys.stderr)
        return 1

    try:
        matrix = build_matrix(sys.argv[1])
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(matrix, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
