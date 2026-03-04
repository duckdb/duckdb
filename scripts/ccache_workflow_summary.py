#!/usr/bin/env python3
"""
Summarize ccache usage for a GitHub Actions workflow run.

Examples:
  python3 scripts/ccache_workflow_summary.py 22331644275
  python3 scripts/ccache_workflow_summary.py 22331644275 --repo duckdb/duckdb --format csv --output summary.csv
  python3 scripts/ccache_workflow_summary.py 22331644275 --keep-retries --verbose
"""

import argparse
import json
import re
import subprocess
import sys
from typing import Dict, List, Optional


HITS_RE = re.compile(r"Hits:\s*\d+\s*/\s*\d+\s*\(\s*([0-9]+(?:\.[0-9]+)?)\s*%\)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze ccache summary for a GitHub Actions run via gh CLI.")
    parser.add_argument("run_id", type=int, help="GitHub Actions run ID (for example: 22331644275).")
    parser.add_argument("--repo", default="duckdb/duckdb", help="Repository in owner/name format.")
    parser.add_argument(
        "--format",
        choices=["markdown", "table", "csv", "json"],
        default="markdown",
        help="Output format (default: markdown).",
    )
    parser.add_argument("--output", default="", help="Optional output file path; otherwise prints to stdout.")
    parser.add_argument(
        "--keep-retries",
        action="store_true",
        help="Include all job attempts. By default retries are collapsed to the latest attempt per job name.",
    )
    parser.add_argument("--verbose", action="store_true", help="Print progress messages to stderr.")
    return parser.parse_args()


def run_gh(args: List[str]) -> str:
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        cmd = " ".join(args)
        raise RuntimeError(f"Command failed ({proc.returncode}): {cmd}\n{proc.stderr.strip()}")
    return proc.stdout


def get_jobs(repo: str, run_id: int) -> List[Dict]:
    output = run_gh(["gh", "run", "view", str(run_id), "--repo", repo, "--json", "jobs"])
    payload = json.loads(output)
    return payload.get("jobs", [])


def dedupe_jobs_keep_latest(jobs: List[Dict]) -> List[Dict]:
    by_name: Dict[str, Dict] = {}
    for job in jobs:
        name = job.get("name", "")
        started_at = job.get("startedAt") or ""
        completed_at = job.get("completedAt") or ""
        dbid = int(job.get("databaseId") or 0)
        candidate_key = (started_at, completed_at, dbid)
        previous = by_name.get(name)
        if previous is None:
            by_name[name] = job
            continue
        prev_key = (
            previous.get("startedAt") or "",
            previous.get("completedAt") or "",
            int(previous.get("databaseId") or 0),
        )
        if candidate_key > prev_key:
            by_name[name] = job
    ordered_names = []
    seen = set()
    for job in jobs:
        name = job.get("name", "")
        if name not in seen:
            ordered_names.append(name)
            seen.add(name)
    return [by_name[name] for name in ordered_names if name in by_name]


def parse_hit_rate_percent(log_text: str) -> Optional[int]:
    in_stats = False
    for line in log_text.splitlines():
        if "ccache -s" in line:
            in_stats = True
            continue
        if in_stats and "ccache --version" in line:
            in_stats = False
            continue
        if not in_stats:
            continue
        match = HITS_RE.search(line)
        if match:
            value = float(match.group(1))
            return int(value + 0.5)
    return None


def parse_found_ccache_file(log_text: str) -> str:
    if "##[group]Restore cache" not in log_text:
        return "N/A"
    if "Cache hit for restore-key" in log_text or "Cache restored successfully" in log_text:
        return "Yes"
    if "No cache found." in log_text:
        return "No"
    return "No"


def format_table(rows: List[Dict[str, str]], columns: List[str]) -> str:
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            widths[col] = max(widths[col], len(str(row.get(col, ""))))
    header = "  ".join(col.ljust(widths[col]) for col in columns)
    sep = "  ".join("-" * widths[col] for col in columns)
    body = ["  ".join(str(row.get(col, "")).ljust(widths[col]) for col in columns) for row in rows]
    return "\n".join([header, sep] + body)


def format_markdown_table(rows: List[Dict[str, str]], columns: List[str]) -> str:
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"
    body = ["| " + " | ".join(str(row.get(col, "")) for col in columns) + " |" for row in rows]
    return "\n".join([header, separator] + body)


def to_csv(rows: List[Dict[str, str]], columns: List[str]) -> str:
    def escape(cell: str) -> str:
        if any(ch in cell for ch in [",", "\"", "\n"]):
            return "\"" + cell.replace("\"", "\"\"") + "\""
        return cell

    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(escape(str(row.get(col, ""))) for col in columns))
    return "\n".join(lines)


def emit(text: str, output_path: str) -> None:
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(text)
            if not text.endswith("\n"):
                f.write("\n")
        return
    print(text)


def summarize_rows(rows: List[Dict[str, str]]) -> str:
    total_jobs = len(rows)
    found_ccache_jobs = sum(1 for row in rows if row.get("found_ccache_file") == "Yes")
    numeric_rates: List[int] = []
    for row in rows:
        rate = row.get("ccache_hit_rate", "")
        if rate.endswith("%"):
            try:
                numeric_rates.append(int(rate[:-1]))
            except ValueError:
                pass
    if numeric_rates:
        avg_rate = int(sum(numeric_rates) / len(numeric_rates) + 0.5)
        avg_text = f"{avg_rate}%"
    else:
        avg_text = "N/A"
    return (
        f"- Total jobs is {total_jobs} where {found_ccache_jobs} use a ccache file.\n"
        f"- The average ccache hit rate is {avg_text}."
    )


def main() -> int:
    args = parse_args()
    try:
        print(
            f"Fetching workflow run log files for run {args.run_id} from {args.repo}...",
            file=sys.stderr,
            flush=True,
        )
        jobs = get_jobs(args.repo, args.run_id)
        selected_jobs = jobs if args.keep_retries else dedupe_jobs_keep_latest(jobs)

        if args.verbose:
            print(
                f"Found {len(jobs)} jobs ({len(selected_jobs)} after retry filtering).",
                file=sys.stderr,
            )

        rows: List[Dict[str, str]] = []
        for job in selected_jobs:
            job_id = int(job.get("databaseId") or 0)
            name = str(job.get("name") or "")
            conclusion = str(job.get("conclusion") or "")
            if args.verbose:
                print(f"Fetching log for job {job_id}: {name}", file=sys.stderr, flush=True)

            try:
                log_text = run_gh(
                    ["gh", "run", "view", str(args.run_id), "--repo", args.repo, "--job", str(job_id), "--log"]
                )
            except RuntimeError:
                hit_rate = "N/A"
                found_ccache = "N/A"
            else:
                parsed_rate = parse_hit_rate_percent(log_text)
                hit_rate = f"{parsed_rate}%" if parsed_rate is not None else "N/A"
                found_ccache = parse_found_ccache_file(log_text)

            rows.append(
                {
                    "job_id": str(job_id),
                    "job_name": name,
                    "conclusion": conclusion,
                    "ccache_hit_rate": hit_rate,
                    "found_ccache_file": found_ccache,
                }
            )
            print(f"Processed job {job_id}: {name}", file=sys.stderr, flush=True)

        rows = [row for row in rows if row.get("conclusion") != "skipped"]
        columns = ["job_id", "job_name", "conclusion", "ccache_hit_rate", "found_ccache_file"]
        if args.format == "json":
            text = json.dumps(rows, indent=2)
        elif args.format == "csv":
            text = to_csv(rows, columns)
        elif args.format == "markdown":
            markdown_columns = ["job_name", "ccache_hit_rate", "found_ccache_file"]
            text = format_markdown_table(rows, markdown_columns) + "\n\n" + summarize_rows(rows)
        else:
            table_columns = ["job_name", "conclusion", "ccache_hit_rate", "found_ccache_file"]
            text = format_table(rows, table_columns) + "\n\n" + summarize_rows(rows)

        emit(text, args.output)
        return 0
    except RuntimeError as err:
        print(str(err), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
