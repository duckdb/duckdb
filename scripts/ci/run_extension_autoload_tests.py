#!/usr/bin/env python3
"""Run configured extension SQL tests with a downloaded DuckDB shell."""

import argparse
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import sys
import tempfile

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import sync_out_of_tree_extensions
from scripts.ci import run_tests

TEST_FLAGS = "--autoloading available --skip-compiled"


def read_extension_configs(repo_root: Path, configs: str, system_name: str | None = None, duckdb_platform: str = ""):
    with tempfile.TemporaryDirectory(prefix="duckdb-extension-test-config-") as directory:
        output = Path(directory) / "extensions.json"
        subprocess.run(
            [
                "cmake",
                f"-DPROJECT_SOURCE_DIR={repo_root}",
                f"-DEXTENSION_CONFIGS={configs}",
                f"-DCMAKE_SYSTEM_NAME={system_name or platform.system()}",
                f"-DMUSL_ENABLED={'1' if 'musl' in duckdb_platform else '0'}",
                f"-DMINGW={'ON' if duckdb_platform.endswith(('_mingw', '_rtools')) else 'OFF'}",
                f"-DOUTPUT_FILE={output}",
                "-P",
                str(REPO_ROOT / "scripts" / "ci" / "export_extension_test_config.cmake"),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(output.read_text(encoding="utf8"))


def prepare_test_roots(repo_root: Path, extensions, build_dir: Path | None = None, *, allow_empty: bool = False):
    resolved_extensions = []
    for extension in extensions:
        extension = dict(extension)
        name = extension["name"]
        if extension["git_url"] and not extension.get("checkout_dir"):
            fetched_source = build_dir / "_deps" / f"{name}_extension_fc-src" if build_dir else None
            if fetched_source and fetched_source.is_dir():
                source = fetched_source
            else:
                sync_out_of_tree_extensions.sync_extension(
                    dict(extension), repo_root / "extension" / "external", repo_root
                )
                source = repo_root / "extension" / "external" / name
            extension["checkout_dir"] = str(source.resolve())
        resolved_extensions.append(extension)
    if not resolved_extensions:
        if allow_empty:
            return []
        raise RuntimeError("No extension SQL test roots selected by the extension configs")
    with tempfile.TemporaryDirectory(prefix="duckdb-extension-paths-") as directory:
        input_file, output_file = Path(directory) / "input.json", Path(directory) / "output.json"
        input_file.write_text(json.dumps(resolved_extensions), encoding="utf8")
        subprocess.run(
            [
                "cmake",
                f"-DPROJECT_SOURCE_DIR={repo_root}",
                f"-DINPUT_FILE={input_file}",
                f"-DOUTPUT_FILE={output_file}",
                "-P",
                str(REPO_ROOT / "scripts/ci/resolve_extension_test_paths.cmake"),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        paths = json.loads(output_file.read_text(encoding="utf8"))
    roots = []
    for extension, path in zip(resolved_extensions, paths):
        name, root = extension["name"], Path(path)
        root = root.resolve()
        if not root.is_dir():
            raise RuntimeError(f"SQL test directory for extension '{name}' does not exist: {root}")
        if root not in roots:
            roots.append(root)
        print(f"Extension SQL tests: {name}: {root}", flush=True)
    if not roots and not allow_empty:
        raise RuntimeError("No extension SQL test roots selected by the extension configs")
    return roots


def run_extension_tests(
    duckdb: Path,
    roots: list[Path],
    repo_root: Path,
    *,
    list_tests: bool = False,
    test_extension: Path | None = None,
    test_flags: str = TEST_FLAGS,
):
    extension = test_extension or run_tests.resolve_test_extension_path(duckdb.parent)
    subprocess.run(
        [
            *run_tests.test_protocol_command(str(duckdb), jsonlines=False),
            "-bail",
            "-c",
            f"LOAD {run_tests.sql_string(extension)};",
        ],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    if not roots:
        print("No extension SQL tests configured; test extension loaded successfully.", flush=True)
        return 0
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "ci" / "run_tests.py"),
        str(duckdb),
        "--test-extension",
        str(extension),
        f"--test-flags={test_flags}",
    ]
    for root in roots:
        command.extend(["--test-root", str(root)])
    listing = subprocess.run([*command, "--list-tests"], cwd=repo_root, capture_output=True, text=True, check=True)
    lines = listing.stdout.splitlines()
    if not lines or lines[0] != "name\tgroup" or len(lines) == 1:
        raise RuntimeError(f"No extension SQL tests selected\n{listing.stdout}\n{listing.stderr}".rstrip())
    print(f"Selected {len(lines) - 1} extension SQL tests from {len(roots)} test roots", flush=True)
    if list_tests:
        print(listing.stdout, end="")
        return 0
    return subprocess.run(command, cwd=repo_root).returncode


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, default=REPO_ROOT / "build" / "release" / "duckdb")
    parser.add_argument("--extension-configs", default=os.environ.get("EXTENSION_CONFIGS", ""))
    parser.add_argument("--source-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--test-root-directory", type=Path)
    parser.add_argument("--test-root", action="append", type=Path, default=[])
    parser.add_argument("--test-extension", type=Path)
    parser.add_argument("--test-flags", default=TEST_FLAGS)
    parser.add_argument("--platform", default="")
    parser.add_argument("--list-tests", action="store_true")
    args = parser.parse_args(argv)
    if not args.extension_configs and not args.test_root:
        parser.error("--extension-configs or --test-root must be set")
    try:
        extensions = (
            read_extension_configs(args.source_root, args.extension_configs, duckdb_platform=args.platform)
            if args.extension_configs
            else []
        )
        roots = prepare_test_roots(args.source_root, extensions, args.duckdb.resolve().parent, allow_empty=True)
        roots = list(dict.fromkeys([*roots, *(root.resolve() for root in args.test_root)]))
        return run_extension_tests(
            args.duckdb.resolve(),
            roots,
            args.test_root_directory or args.source_root,
            list_tests=args.list_tests,
            test_extension=args.test_extension,
            test_flags=args.test_flags,
        )
    except subprocess.CalledProcessError as error:
        print(f"error: {shlex.join(map(str, error.cmd))} exited with status {error.returncode}", file=sys.stderr)
        if error.stdout:
            print(error.stdout, end="", file=sys.stderr)
        if error.stderr:
            print(error.stderr, end="", file=sys.stderr)
        return 1
    except (OSError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
