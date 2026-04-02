#!/usr/bin/env python3

import argparse
import json
import os
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
    "amalgamation-tests",
    "linux-configs",
]

NIGHTLY_JOBS = [
    "main_julia",
    "valgrind",
]

MERGE_GROUP_JOBS = [
    "linux-debug",
    "linux-release",
    "tidy-check",
]

PREPARE_JOBS = [
    "prepare",
]

ALL_JOBS = set(PREPARE_JOBS) | set(PULL_REQUEST_JOBS) | set(NIGHTLY_JOBS) | set(MERGE_GROUP_JOBS)


@dataclass(frozen=True)
class JobSelection:
    enabled_jobs: list[str]
    save_cache: bool


def should_save_cache(event_name: str, ref_name: str, repository: str) -> bool:
    return (
        repository != "duckdb/duckdb"
        or ref_name == "main"
        or ref_name == "v1.5-variegata"
        or event_name == "merge_group"
    )


def enabled_jobs(event_name: str, ref_name: str) -> list[str]:
    if event_name == "merge_group":
        return MERGE_GROUP_JOBS.copy()

    # Keep workflow_dispatch and push aligned.
    if event_name in {"push", "workflow_dispatch"}:
        jobs = PULL_REQUEST_JOBS.copy()
        if ref_name == "main":
            jobs.extend(NIGHTLY_JOBS)
        return jobs

    jobs = PULL_REQUEST_JOBS.copy()
    if ref_name == "main":
        jobs.extend(NIGHTLY_JOBS)
    return jobs


def compute_job_selection(event_name: str, ref_name: str, repository: str) -> JobSelection:
    return JobSelection(
        enabled_jobs=enabled_jobs(event_name, ref_name),
        save_cache=should_save_cache(event_name, ref_name, repository),
    )


def write_outputs(selection: JobSelection, out: TextIO) -> None:
    out.write(f"enabled_jobs={json.dumps(selection.enabled_jobs, separators=(',', ':'))}\n")
    out.write(f"save_cache={'true' if selection.save_cache else 'false'}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute enabled Main CI jobs and cache save policy.")
    parser.add_argument("--event", dest="event_name", required=True)
    parser.add_argument("--ref_name", required=True)
    parser.add_argument("--repository", default="duckdb/duckdb")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    selection = compute_job_selection(args.event_name, args.ref_name, args.repository)

    # Emit to stderr so helper output stays visible in CI logs without polluting stdout pipelines.
    write_outputs(selection, sys.stderr)

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            write_outputs(selection, f)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
