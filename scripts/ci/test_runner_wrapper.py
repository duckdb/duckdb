#!/usr/bin/env python3

import os
import sys
from contextlib import contextmanager
from pathlib import Path


def resolve_unittest_path(wrapper_path: str | Path, unittest_name: str) -> Path:
    return Path(wrapper_path).resolve().parent / unittest_name


def build_run_tests_argv(
    forwarded_args: list[str], wrapper_path: str | Path, unittest_name: str = "unittest"
) -> list[str]:
    unittest_path = resolve_unittest_path(wrapper_path, unittest_name)
    return [os.fspath(unittest_path), *forwarded_args]


@contextmanager
def pushd(path: str | Path):
    old_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old_cwd)


def main(
    forwarded_args: list[str] | None = None,
    *,
    wrapper_path: str | Path | None = None,
    source_root: str | Path | None = None,
    unittest_name: str = "unittest",
) -> int:
    if forwarded_args is None:
        forwarded_args = sys.argv[1:]
    if wrapper_path is None:
        wrapper_path = sys.argv[0]
    if source_root is None:
        raise ValueError("source_root is required")

    source_root_path = Path(source_root).resolve()
    if os.fspath(source_root_path) not in sys.path:
        sys.path.insert(0, os.fspath(source_root_path))

    unittest_path = resolve_unittest_path(wrapper_path, unittest_name)
    if not unittest_path.is_file():
        print(f"error: expected sibling unittest binary at {unittest_path}", file=sys.stderr)
        return 1

    from scripts.ci import run_tests

    with pushd(source_root_path):
        return int(run_tests.main(build_run_tests_argv(forwarded_args, wrapper_path, unittest_name)) or 0)
