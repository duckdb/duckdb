#!/usr/bin/env python3

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import TextIO

COMMON_JOBS = [
    "linux-relassert",
    "linux-relassert-tests",
    "tidy-check",
    "extensions",
    "wasm-eh",
    "linux-release",
    "linux-release-tests",
    "linux-release-musl",
    "swift",
    "windows",
    "no-string-inline",
    "no-rtti",
    "vector-sizes",
    "threadsan",
    "linux-configs",
]

PULL_REQUEST_ONLY_JOBS = [
    "regression",
]

PULL_REQUEST_JOBS = COMMON_JOBS + PULL_REQUEST_ONLY_JOBS

NIGHTLY_ONLY_JOBS = [
    "osx",
    "codecov",
]

NIGHTLY_JOBS = COMMON_JOBS + NIGHTLY_ONLY_JOBS

MERGE_GROUP_JOBS = [
    "linux-relassert",
    "linux-release",
    "linux-release-tests",
    "tidy-check",
]

RELEASE_JOBS = [
    "osx",
    "staged-extension-install",
]

SKIP_TESTS_JOBS = {
    "linux-relassert-tests",
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

ALL_JOBS = set(PREPARE_JOBS)
ALL_JOBS |= set(PULL_REQUEST_JOBS)
ALL_JOBS |= set(NIGHTLY_JOBS)
ALL_JOBS |= set(MERGE_GROUP_JOBS)
ALL_JOBS |= set(SUMMARY_JOBS)
ALL_JOBS |= set(RELEASE_JOBS)
SELECTABLE_JOBS = ALL_JOBS - set(SUMMARY_JOBS)


@dataclass(frozen=True)
class JobSelection:
    enabled_jobs: list[str]
    save_cache: bool
    optimized_release: bool = False
    linux_release_matrix: list[dict[str, object]] = field(default_factory=list)
    linux_musl_matrix: list[dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class JobSelectionInput:
    event_name: str
    ref_name: str
    repository: str
    skip_tests: bool
    changed_keys: set[str]
    runners: dict[str, str] = field(default_factory=dict)


def should_save_cache(selection_input: JobSelectionInput) -> bool:
    return (
        selection_input.repository != "duckdb/duckdb"
        or selection_input.ref_name == "main"
        or selection_input.ref_name == "v1.5-variegata"
        or selection_input.event_name == "merge_group"
    )


def enabled_jobs(selection_input: JobSelectionInput) -> list[str]:
    if selection_input.event_name == "merge_group":
        selected_jobs = MERGE_GROUP_JOBS.copy()
    elif selection_input.ref_name == "main":
        selected_jobs = NIGHTLY_JOBS.copy()
    else:
        selected_jobs = PULL_REQUEST_JOBS.copy()

    if selection_input.event_name == "workflow_dispatch":
        selected_jobs = [job for job in selected_jobs if job != "regression"]
        selected_jobs.extend(RELEASE_JOBS)

    if selection_input.skip_tests:
        selected_jobs = [job for job in selected_jobs if job not in SKIP_TESTS_JOBS]

    if (
        selection_input.event_name in {"push", "pull_request"}
        and "osx" in selection_input.changed_keys
        and "osx" not in selected_jobs
    ):
        selected_jobs.append("osx")

    override = parse_job_selection_override(os.getenv("OVERRIDE_JOBS"))
    if override is not None:
        return override

    return selected_jobs


def parse_job_selection_override(value: str | None) -> list[str] | None:
    if value is None:
        return None
    parsed_jobs = [token for token in re.split(r"[\s,]+", value.strip()) if token]
    if not parsed_jobs:
        return PREPARE_JOBS.copy()

    invalid_jobs = sorted(set(parsed_jobs) - SELECTABLE_JOBS)
    if invalid_jobs:
        raise ValueError(f"invalid jobs in OVERRIDE_JOBS: {', '.join(invalid_jobs)}")

    seen: set[str] = set()
    deduplicated = [job for job in parsed_jobs if not (job in seen or seen.add(job))]
    if "prepare" not in seen:
        deduplicated = PREPARE_JOBS + deduplicated
    return deduplicated


def compatibility_release_config(*, runner: str, arch: str, optimized_release: bool) -> dict[str, object]:
    is_amd64 = arch == "amd64"
    artifact_suffix = f"linux-{arch}"
    if is_amd64:
        build_artifact = "linux-release-compat-build" if optimized_release else "linux-release-build"
    else:
        build_artifact = "linux-release-arm64-compat-build" if optimized_release else "linux-release-arm64-build"

    return {
        "runner": runner,
        "arch": arch,
        "image": f"manylinux_{arch}_main",
        "name": f"{arch} compatibility",
        "artifact_suffix": artifact_suffix,
        "build_artifact": build_artifact,
        "extension_config": "linux_release_extensions.cmake" if is_amd64 else "bundled_extensions.cmake",
        "vcpkg_cache_suffix": "" if is_amd64 else "linux-cli-arm64-glibc",
        "build_jemalloc": "1" if is_amd64 else "0",
        "use_ccache": True,
        "ccache_key": f"linux-cli-{arch}-glibc",
        "save_vcpkg_cache": True,
        "lto": "",
        "lto_jobs": "",
        "extra_cmake_variables": "",
        "is_compatibility_build": True,
        "is_canonical_build": not optimized_release,
        "publish_static": True,
        "publish_source": is_amd64,
        "run_smoke": is_amd64,
        "run_arm_tests": not is_amd64 and not optimized_release,
    }


def optimized_release_config(*, runner: str, arch: str) -> dict[str, object]:
    is_amd64 = arch == "amd64"
    return {
        "runner": runner,
        "arch": arch,
        "image": f"manylinux_{arch}_main",
        "name": f"{arch} optimized",
        "artifact_suffix": f"linux-{arch}",
        "build_artifact": "linux-release-build" if is_amd64 else "linux-release-arm64-build",
        "extension_config": "linux_release_extensions.cmake" if is_amd64 else "bundled_extensions.cmake",
        "vcpkg_cache_suffix": "" if is_amd64 else "linux-cli-arm64-glibc",
        "build_jemalloc": "1" if is_amd64 else "0",
        "use_ccache": False,
        "ccache_key": "",
        "save_vcpkg_cache": False,
        "lto": "thin",
        "lto_jobs": "8",
        "extra_cmake_variables": "-DCMAKE_C_COMPILER_LAUNCHER= -DCMAKE_CXX_COMPILER_LAUNCHER=",
        "is_compatibility_build": False,
        "is_canonical_build": True,
        "publish_static": False,
        "publish_source": False,
        "run_smoke": True,
        "run_arm_tests": not is_amd64,
    }


def linux_release_matrix(selection_input: JobSelectionInput, optimized_release: bool) -> list[dict[str, object]]:
    result = [
        compatibility_release_config(
            runner=selection_input.runners.get("linux_x64", ""), arch="amd64", optimized_release=optimized_release
        )
    ]
    if selection_input.event_name not in {"pull_request", "merge_group"}:
        result.append(
            compatibility_release_config(
                runner=selection_input.runners.get("linux_arm64", ""),
                arch="arm64",
                optimized_release=optimized_release,
            )
        )
    if optimized_release:
        result.extend(
            [
                optimized_release_config(runner=selection_input.runners.get("linux_x64", ""), arch="amd64"),
                optimized_release_config(runner=selection_input.runners.get("linux_arm64", ""), arch="arm64"),
            ]
        )
    return result


def linux_musl_matrix(selection_input: JobSelectionInput) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    if selection_input.event_name != "pull_request":
        result.append(
            {
                "runner": selection_input.runners.get("linux_x64", ""),
                "arch": "amd64",
                "image": "alpine_amd64_main",
                "name": "amd64",
                "artifact_suffix": "linux-amd64-musl",
                "cache_suffix": "amd64-musl",
            }
        )
    result.append(
        {
            "runner": selection_input.runners.get("linux_arm64", ""),
            "arch": "arm64",
            "image": "alpine_arm64_main",
            "name": "arm64",
            "artifact_suffix": "linux-arm64-musl",
            "cache_suffix": "arm64-musl",
        }
    )
    return result


def compute_job_selection(selection_input: JobSelectionInput) -> JobSelection:
    selected_jobs = enabled_jobs(selection_input)
    optimized_release = selection_input.event_name == "workflow_dispatch" and "linux-release" in selected_jobs
    return JobSelection(
        enabled_jobs=selected_jobs,
        save_cache=should_save_cache(selection_input),
        optimized_release=optimized_release,
        linux_release_matrix=linux_release_matrix(selection_input, optimized_release),
        linux_musl_matrix=linux_musl_matrix(selection_input),
    )


def write_outputs(selection: JobSelection, out: TextIO, *, include_matrices: bool = True) -> None:
    out.write(f"enabled_jobs={json.dumps(selection.enabled_jobs, separators=(',', ':'))}\n")
    out.write(f"save_cache={'true' if selection.save_cache else 'false'}\n")
    out.write(f"optimized_release={'true' if selection.optimized_release else 'false'}\n")
    if include_matrices:
        out.write(f"linux_release_matrix={json.dumps(selection.linux_release_matrix, separators=(',', ':'))}\n")
        out.write(f"linux_musl_matrix={json.dumps(selection.linux_musl_matrix, separators=(',', ':'))}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute enabled Main CI jobs and cache save policy.")
    parser.add_argument("--event", dest="event_name", required=True)
    parser.add_argument("--ref_name", required=True)
    parser.add_argument("--repository", default="duckdb/duckdb")
    parser.add_argument("--skip-tests", default="false")
    parser.add_argument("--changed-keys", default="")
    parser.add_argument("--runners", required=True)
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


def parse_runners(value: str) -> dict[str, str]:
    parsed = json.loads(value)
    if not isinstance(parsed, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in parsed.items()
    ):
        raise ValueError("runners must be a JSON object with string keys and values")
    missing = {"linux_x64", "linux_arm64"} - parsed.keys()
    if missing:
        raise ValueError(f"runners is missing required keys: {', '.join(sorted(missing))}")
    return parsed


def main() -> int:
    args = parse_args()
    selection_input = JobSelectionInput(
        event_name=args.event_name,
        ref_name=args.ref_name,
        repository=args.repository,
        skip_tests=parse_bool(args.skip_tests),
        changed_keys=parse_changed_keys(args.changed_keys),
        runners=parse_runners(args.runners),
    )
    selection = compute_job_selection(selection_input)

    # Emit to stderr so helper output stays visible in CI logs without polluting stdout pipelines.
    write_outputs(selection, sys.stderr, include_matrices=False)

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            write_outputs(selection, f)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
