#!/usr/bin/env python3
"""
Convert `gh cache list` output into a detailed CSV with bucket mappings.

Example: generate the CSV directly from GitHub cache output

    gh cache list --repo duckdb/duckdb --limit 1000 | \
      python3 scripts/github_cache_usage.py

Example: summarize entry count, total GiB, and percentage by bucket

    duckdb -c "
      SELECT
        bucket,
        COUNT(*) AS entry_count,
        ROUND(SUM(size_gib), 3) AS total_gib,
        ROUND(100.0 * SUM(size_gib) / SUM(SUM(size_gib)) OVER ()) AS pct_of_total,
        MAX(last_accessed_at) AS last_usage,
      FROM read_csv_auto('github_cache_usage.csv')
      GROUP BY bucket
      ORDER BY total_gib DESC;
    "
"""

import argparse
import sys
import csv
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

MAIN = {
    "linux-debug",
    "linux-configs",
    "vector-sizes",
    "valgrind",
    "threadsan",
    "no-string-inline",
}

NIGHTLYTESTS = {
    "release-assert",
    "release-assert-clang",
    "release-assert-osx-storage",
    "smaller-binary",
    "hash-zero",
    "extension-updating",
    "regression-test-memory-safety",
    "vector-and-block-sizes",
    "linux-debug-configs",
    "force-blocking-sink-source",
    "storage-initialization",
}

EXTENSIONS_EXPLICIT = {
    "autoload-tests",
    "check-load-install-extensions",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse `gh cache list` from stdin and classify github cache items to CI workflows."
    )
    parser.add_argument(
        "--output",
        default="github_cache_usage.csv",
        help="Output CSV path (default: github_cache_usage.csv).",
    )
    parser.add_argument(
        "--workflows-dir",
        default=".github/workflows",
        help="Workflow directory used to map ccache keys to jobs/workflows.",
    )
    return parser.parse_args()


def parse_size_to_gib(size_raw: str) -> float:
    parts = size_raw.strip().split()
    if len(parts) != 2:
        return 0.0
    value_str, unit = parts
    try:
        value = float(value_str)
    except ValueError:
        return 0.0
    if unit == "GiB":
        return value
    if unit == "MiB":
        return value / 1024.0
    if unit == "KiB":
        return value / (1024.0 * 1024.0)
    return 0.0


def normalize_name(cache_key: str) -> str:
    name = re.sub(r"-202\d-\d{2}-\d{2}.*$", "", cache_key)
    while name.startswith("ccache-"):
        name = name[len("ccache-") :]
    return name


def classify_bucket(normalized: str) -> Tuple[str, str]:
    if normalized.startswith("extension-distribution-"):
        return "extensions", "prefix rule: extension-distribution-*"
    if normalized in EXTENSIONS_EXPLICIT:
        return "extensions", "explicit extensions allowlist"
    if normalized in MAIN:
        return "main", "explicit main job list"
    if normalized in NIGHTLYTESTS:
        return "nightlytests", "explicit nightlytests job list"
    if normalized in {"xcode-debug", "xcode-release"}:
        return "osx", "explicit osx job list"
    if normalized == "bundle-osx-static-libs":
        return "bundlestaticlibs", "explicit bundle static libs job"
    if normalized == "linux-release":
        return "linux-release", "explicit linux-release job list"
    return "unmapped/other", "no mapping rule matched"


def extract_ccache_job_mapping(workflows_dir: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for workflow in sorted(workflows_dir.glob("*.yml")):
        current_job: Optional[str] = None
        in_jobs = False
        for raw_line in workflow.read_text(encoding="utf-8").splitlines():
            line = raw_line.rstrip("\n")
            if line.startswith("jobs:"):
                in_jobs = True
                current_job = None
                continue
            if in_jobs and line and not line.startswith(" "):
                in_jobs = False
                current_job = None
                continue
            if not in_jobs:
                continue
            match = re.match(r"^(\s{1,2})([A-Za-z0-9_-]+):\s*$", line)
            if match:
                current_job = match.group(2)
                continue
            if "uses:" in line and "ccache-action" in line and current_job:
                mapping.setdefault(current_job, str(workflow))
    return mapping


def parse_gh_cache_lines(lines: Iterable[str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for line in lines:
        text = line.strip()
        if not text:
            continue
        parts = text.split("\t")
        if len(parts) < 5:
            continue
        cache_id, cache_key, size_raw, created_at, last_accessed_at = parts[:5]
        rows.append(
            {
                "cache_id": cache_id,
                "cache_key": cache_key,
                "size_raw": size_raw,
                "created_at": created_at,
                "last_accessed_at": last_accessed_at,
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    workflows_dir = Path(args.workflows_dir)

    cache_lines = sys.stdin.read().splitlines()
    parsed_rows = parse_gh_cache_lines(cache_lines)
    job_to_workflow = extract_ccache_job_mapping(workflows_dir)

    fieldnames = [
        "cache_id",
        "cache_key",
        "size_raw",
        "size_gib",
        "created_at",
        "last_accessed_at",
        "normalized_name",
        "bucket",
        "workflow_file",
        "mapped_job",
        "mapping_reason",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        for row in parsed_rows:
            normalized = normalize_name(row["cache_key"])
            bucket, rule_reason = classify_bucket(normalized)
            mapped_job = normalized if normalized in job_to_workflow else ""
            workflow_file = job_to_workflow.get(normalized, "")
            reasons = [rule_reason]
            if mapped_job:
                reasons.append("job-name match")
            if row["cache_key"].startswith("ccache-ccache-"):
                reasons.append("double-ccache normalized")

            writer.writerow(
                {
                    "cache_id": row["cache_id"],
                    "cache_key": row["cache_key"],
                    "size_raw": row["size_raw"],
                    "size_gib": f"{parse_size_to_gib(row['size_raw']):.6f}",
                    "created_at": row["created_at"],
                    "last_accessed_at": row["last_accessed_at"],
                    "normalized_name": normalized,
                    "bucket": bucket,
                    "workflow_file": workflow_file,
                    "mapped_job": mapped_job,
                    "mapping_reason": "; ".join(reasons),
                }
            )
    print("wrote CSV to {}".format(str(output_path)))


if __name__ == "__main__":
    main()
