#!/usr/bin/env python3
"""Build an AFL++ corpus from test/**/*.test files.

Workflow:
1. Group test files by a configurable number of parent parts under test/ (default depth 2, e.g. test/a/b/c.test -> a_b).
2. Copy grouped seeds into per-group raw directories.
3. Run afl-cmin per group in parallel.
4. Merge all minimized files into one flat final directory.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


DEFAULT_TARGET = "build/fuzzer/test/unittest"
DEFAULT_GLOB_PATTERN = "test/**/*.test"
DEFAULT_AFL_CMIN_BIN = "afl-cmin"
DEFAULT_JOBS = max(1, (os.cpu_count() or 1))
DEFAULT_GROUP_DEPTH = 2


@dataclass(frozen=True)
class GroupTask:
    name: str
    raw_dir: Path
    min_dir: Path


@dataclass(frozen=True)
class CorpusConfig:
    output_dir: Path
    glob_pattern: str = DEFAULT_GLOB_PATTERN
    jobs: int = DEFAULT_JOBS
    target: str = DEFAULT_TARGET
    afl_cmin_cmd: str = DEFAULT_AFL_CMIN_BIN
    group_depth: int = DEFAULT_GROUP_DEPTH


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a grouped + minimized AFL++ corpus from test/**/*.test")
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        required=True,
        help="Output root directory",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=DEFAULT_JOBS,
        help="Parallel afl-cmin jobs (default: CPU count)",
    )
    parser.add_argument(
        "--glob",
        default=DEFAULT_GLOB_PATTERN,
        help=f"Glob pattern for test files (default: {DEFAULT_GLOB_PATTERN})",
    )
    parser.add_argument(
        "--target",
        default=DEFAULT_TARGET,
        help=f"Fuzzer target command (binary plus optional args) as one string (default: {DEFAULT_TARGET})",
    )
    parser.add_argument(
        "--afl-cmin",
        default=DEFAULT_AFL_CMIN_BIN,
        help=f"afl-cmin executable and optional leading flags as one string (default: {DEFAULT_AFL_CMIN_BIN})",
    )
    parser.add_argument(
        "--group-depth",
        type=int,
        default=DEFAULT_GROUP_DEPTH,
        help=f"How many parent path parts under test/ are used for the group key (default: {DEFAULT_GROUP_DEPTH})",
    )
    return parser.parse_args()


def list_test_files(glob_pattern: str) -> list[Path]:
    return sorted(path for path in Path().glob(glob_pattern) if path.is_file())


def group_name_for(test_file: Path, config: CorpusConfig) -> str:
    parts = test_file.parts
    try:
        test_index = parts.index("test")
    except ValueError as ex:
        raise ValueError(f"Expected test file path under test/: {test_file}") from ex

    rel = Path(*parts[test_index + 1 :])
    parent_parts = rel.parts[:-1]
    selected_parts = parent_parts[: config.group_depth]
    if not selected_parts:
        return "root"
    return "_".join(selected_parts)


def ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def stage_grouped_raw_corpus(test_files: list[Path], raw_root: Path, config: CorpusConfig) -> dict[str, Path]:
    groups: dict[str, Path] = {}
    for test_file in test_files:
        group = group_name_for(test_file, config)
        group_dir = raw_root / group
        group_dir.mkdir(parents=True, exist_ok=True)
        destination = group_dir / test_file.name
        shutil.copy2(test_file, destination)
        groups[group] = group_dir
    return groups


def normalize_afl_cmin_cmd(afl_cmin: str) -> list[str]:
    cmd = shlex.split(afl_cmin)
    if not cmd:
        raise ValueError("afl-cmin command must include a binary")
    return cmd


def normalize_target_cmd(target: str) -> list[str]:
    cmd = shlex.split(target)
    if not cmd:
        raise ValueError("target command must include a binary")
    return cmd


def run_afl_cmin(task: GroupTask, afl_cmin_cmd: list[str], config: CorpusConfig) -> tuple[str, int, str, str]:
    target_cmd = normalize_target_cmd(config.target)
    cmd = [
        *afl_cmin_cmd,
        "-i",
        str(task.raw_dir),
        "-o",
        str(task.min_dir),
        "--",
        *target_cmd,
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    return task.name, proc.returncode, proc.stdout, proc.stderr


def merge_minimized(min_root: Path, final_dir: Path) -> int:
    final_dir.mkdir(parents=True, exist_ok=True)
    seen: dict[str, int] = {}
    copied = 0

    for group_dir in sorted(path for path in min_root.iterdir() if path.is_dir()):
        group = group_dir.name
        for src in sorted(path for path in group_dir.iterdir() if path.is_file()):
            base_name = f"{group}__{src.name}"
            count = seen.get(base_name, 0)
            seen[base_name] = count + 1

            if count == 0:
                target_name = base_name
            else:
                target_name = f"{group}__{count}__{src.name}"

            shutil.copy2(src, final_dir / target_name)
            copied += 1

    return copied


def validate_environment(afl_cmin_cmd: list[str], target_cmd: list[str]) -> None:
    afl_cmin_bin = afl_cmin_cmd[0]
    if shutil.which(afl_cmin_bin) is None:
        raise RuntimeError(f"{afl_cmin_bin} not found in PATH")
    target_bin = target_cmd[0]
    target_path = Path(target_bin)
    if not target_path.exists() and shutil.which(target_bin) is None:
        raise RuntimeError(f"Fuzzer target missing at {target_bin}. Build it first with `make fuzzer`.")


def run(config: CorpusConfig) -> int:
    if config.jobs < 1:
        print("--jobs must be >= 1", file=sys.stderr)
        return 2

    if not Path("test").exists():
        print("test/ directory not found", file=sys.stderr)
        return 2

    try:
        afl_cmin_cmd = normalize_afl_cmin_cmd(config.afl_cmin_cmd)
    except ValueError as ex:
        print(str(ex), file=sys.stderr)
        return 2

    try:
        target_cmd = normalize_target_cmd(config.target)
    except ValueError as ex:
        print(str(ex), file=sys.stderr)
        return 2

    try:
        validate_environment(afl_cmin_cmd, target_cmd)
    except RuntimeError as ex:
        print(str(ex), file=sys.stderr)
        return 2

    test_files = list_test_files(config.glob_pattern)
    if not test_files:
        print(f"No test files found for glob: {config.glob_pattern}", file=sys.stderr)
        return 2

    output_root = config.output_dir
    output_root.mkdir(parents=True, exist_ok=True)

    ensure_clean_dir(output_root)

    with tempfile.TemporaryDirectory(prefix="corpus_work_") as workdir:
        work_root = Path(workdir)
        raw_root = work_root / "raw"
        min_root = work_root / "min"
        raw_root.mkdir(parents=True, exist_ok=True)
        min_root.mkdir(parents=True, exist_ok=True)

        grouped_raw = stage_grouped_raw_corpus(test_files, raw_root, config)
        tasks = [
            GroupTask(name=group, raw_dir=raw_dir, min_dir=min_root / group)
            for group, raw_dir in sorted(grouped_raw.items())
        ]

        print(f"Discovered {len(test_files)} test files across {len(tasks)} groups")
        print(f"Running afl-cmin with {config.jobs} parallel jobs")

        failed: list[str] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.jobs) as executor:
            futures = [executor.submit(run_afl_cmin, task, afl_cmin_cmd, config) for task in tasks]
            for future in concurrent.futures.as_completed(futures):
                group, returncode, stdout, stderr = future.result()
                if returncode != 0:
                    failed.append(group)
                    print(f"[FAIL] group={group}", file=sys.stderr)
                    if stdout:
                        print(stdout.rstrip(), file=sys.stderr)
                    if stderr:
                        print(stderr.rstrip(), file=sys.stderr)
                else:
                    print(f"[OK] group={group}")

        if failed:
            print(f"[!] some afl-cmin runs {len(failed)} failed: {failed}", file=sys.stderr)

        copied = merge_minimized(min_root, output_root)
        print(f"Merged {copied} minimized files into {output_root}")

    return 0


def main() -> int:
    args = parse_args()
    config = CorpusConfig(
        output_dir=args.output_dir,
        glob_pattern=args.glob,
        jobs=args.jobs,
        target=args.target,
        afl_cmin_cmd=args.afl_cmin,
        group_depth=args.group_depth,
    )
    return run(config)


if __name__ == "__main__":
    raise SystemExit(main())
