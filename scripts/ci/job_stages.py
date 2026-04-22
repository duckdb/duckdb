#!/usr/bin/env python3

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import TextIO

PULL_REQUEST_JOBS = [
    "linux-debug",
    "linux-debug-tests",
    "regression",
    "tidy-check",
    "extensions",
    "wasm-eh",
    "linux-release",
    "linux-release-tests",
    "linux-release-cli",
    "linux-musl-release-cli",
    "upload-libduckdb-src",
    "swift",
    "windows",
    "no-string-inline",
    "vector-sizes",
    "threadsan",
    "linux-configs",
]

NIGHTLY_ONLY_JOBS = [
    "main_julia",
    "check-clangd-tidy",
    "valgrind",
]

NIGHTLY_JOBS = PULL_REQUEST_JOBS + NIGHTLY_ONLY_JOBS

MERGE_GROUP_JOBS = [
    "linux-debug",
    "linux-release",
    "linux-release-tests",
    "check-clangd-tidy",
    "tidy-check",
]

SKIP_TESTS_JOBS = {
    "linux-debug-tests",
    "regression",
    "swift",
    "linux-configs",
    "linux-release-tests",
}

PREPARE_JOBS = [
    "prepare",
]

SUMMARY_JOBS = [
    "summary",
]

ALL_JOBS = set(PREPARE_JOBS) | set(PULL_REQUEST_JOBS) | set(NIGHTLY_JOBS) | set(MERGE_GROUP_JOBS) | set(SUMMARY_JOBS)


@dataclass(frozen=True)
class JobSelection:
    enabled_jobs: list[str]
    save_cache: bool


@dataclass(frozen=True)
class JobSelectionInput:
    event_name: str
    ref_name: str
    repository: str
    skip_tests: bool
    changed_keys: set[str]


def should_save_cache(selection_input: JobSelectionInput) -> bool:
    return (
        selection_input.repository != "duckdb/duckdb"
        or selection_input.ref_name == "main"
        or selection_input.ref_name == "v1.5-variegata"
        or (selection_input.event_name == "push" and selection_input.ref_name.startswith("gh-readonly-queue/"))
    )


def enabled_jobs(selection_input: JobSelectionInput) -> list[str]:
    if selection_input.event_name == "push" and selection_input.ref_name.startswith("gh-readonly-queue/"):
        selected_jobs = MERGE_GROUP_JOBS.copy()
    elif selection_input.ref_name == "main":
        selected_jobs = NIGHTLY_JOBS.copy()
    else:
        selected_jobs = PULL_REQUEST_JOBS.copy()

    if selection_input.skip_tests:
        selected_jobs = [job for job in selected_jobs if job not in SKIP_TESTS_JOBS]

    if "julia" in selection_input.changed_keys:
        selected_jobs.append("main_julia")

    return selected_jobs


def compute_job_selection(selection_input: JobSelectionInput) -> JobSelection:
    return JobSelection(
        enabled_jobs=enabled_jobs(selection_input),
        save_cache=should_save_cache(selection_input),
    )


def write_outputs(selection: JobSelection, out: TextIO) -> None:
    out.write(f"enabled_jobs={json.dumps(selection.enabled_jobs, separators=(',', ':'))}\n")
    out.write(f"save_cache={'true' if selection.save_cache else 'false'}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute enabled Main CI jobs and cache save policy.")
    parser.add_argument("--event", dest="event_name", required=True)
    parser.add_argument("--ref_name", required=True)
    parser.add_argument("--repository", default="duckdb/duckdb")
    parser.add_argument("--skip-tests", default="false")
    parser.add_argument("--changed-keys", default="")
    return parser.parse_args()


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def parse_changed_keys(value: str) -> set[str]:
    # changed-files may emit keys separated by spaces/newlines, and can be configured
    # to use commas. Support both delimiters defensively.
    return {token.lower() for token in re.split(r"[\s,]+", value.strip()) if token}


def main() -> int:
    args = parse_args()
    selection_input = JobSelectionInput(
        event_name=args.event_name,
        ref_name=args.ref_name,
        repository=args.repository,
        skip_tests=parse_bool(args.skip_tests),
        changed_keys=parse_changed_keys(args.changed_keys),
    )
    selection = compute_job_selection(selection_input)

    # Emit to stderr so helper output stays visible in CI logs without polluting stdout pipelines.
    write_outputs(selection, sys.stderr)

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            write_outputs(selection, f)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
