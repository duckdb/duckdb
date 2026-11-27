#!/usr/bin/env python3
"""Check that affected extensions' patches apply to their configured revisions."""

import argparse
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile

REPO_ROOT = Path(__file__).resolve().parents[2]
APPLY_PATCHES_SCRIPT = REPO_ROOT / "scripts" / "apply_extension_patches.py"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.sync_out_of_tree_extensions import parse_cmake_file


def collect_patched_extensions(repo_root):
    config_dir = repo_root / ".github" / "config"
    extensions = {}
    for config in sorted(config_dir.rglob("*.cmake")):
        for extension in parse_cmake_file(config, config_dir / "extensions"):
            if extension["apply_patches"]:
                key = (
                    extension["name"],
                    extension["git_url"],
                    extension["git_tag"],
                    tuple(extension["submodules"]),
                )
                extensions.setdefault(key, extension)
    return list(extensions.values())


def select_extensions(repo_root, changed_files, extensions):
    names = set()
    for filename in changed_files:
        path = Path(filename).as_posix()
        patch_match = re.fullmatch(r"\.github/patches/extensions/([^/]+)/.+", path)
        if patch_match:
            names.add(patch_match[1])
            continue
        config_match = re.fullmatch(r"\.github/config/extensions/([^/]+)\.cmake", path)
        if config_match:
            names.add(config_match[1])
            config = repo_root / path
            if config.is_file():
                names.update(
                    extension["name"]
                    for extension in parse_cmake_file(config, repo_root / ".github" / "config" / "extensions")
                )
            continue
        print(f"Checking all patched extensions for shared or unknown path: {filename}", flush=True)
        return extensions
    return [extension for extension in extensions if extension["name"] in names]


def run_command(command, cwd=None, env=None):
    return subprocess.run(
        command, cwd=cwd, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True, check=True
    )


def check_extension(repo_root, extension):
    name = extension["name"]
    print(f"Checking patches for {name} @ {extension['git_tag']}", flush=True)
    with tempfile.TemporaryDirectory(prefix=f"duckdb-check-patches-{name}-") as directory:
        checkout = Path(directory)
        run_command(["git", "init", "--quiet"], cwd=checkout)
        run_command(["git", "remote", "add", "origin", extension["git_url"]], cwd=checkout)
        run_command(["git", "fetch", "--quiet", "--depth=1", "--no-tags", "origin", extension["git_tag"]], cwd=checkout)
        run_command(["git", "checkout", "--quiet", "--detach", "FETCH_HEAD"], cwd=checkout)
        if extension["submodules"]:
            run_command(
                ["git", "submodule", "update", "--init", "--recursive", "--depth=1", "--", *extension["submodules"]],
                cwd=checkout,
            )
        patch_dir = (repo_root / ".github" / "patches" / "extensions" / name).resolve()
        result = run_command(
            [sys.executable, str(APPLY_PATCHES_SCRIPT), str(patch_dir)],
            cwd=checkout,
            env={**os.environ, "DUCKDB_SKIP_APPLYING_PATCHES": "0"},
        )
        print(result.stdout, end="", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--changed-files", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        changed_files = [line for line in args.changed_files.read_text(encoding="utf8").splitlines() if line]
        extensions = select_extensions(REPO_ROOT, changed_files, collect_patched_extensions(REPO_ROOT))
        for extension in extensions:
            check_extension(REPO_ROOT, extension)
        print(f"Successfully checked patches for {len(extensions)} extension declarations.", flush=True)
        return 0
    except subprocess.CalledProcessError as error:
        print(f"error: {shlex.join(map(str, error.cmd))} exited with status {error.returncode}", file=sys.stderr)
        if error.stdout:
            print(error.stdout, end="", file=sys.stderr)
        if error.stderr:
            print(error.stderr, end="", file=sys.stderr)
        return 1
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
