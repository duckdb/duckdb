#!/usr/bin/env python3
"""
Differential PEG parser test against the full DuckDB test suite.

For each SQL statement extracted from .test files, run it through:
  - default parser:  SQL | ./duckdb
  - PEG parser:      LOAD ext; CALL enable_peg_parser(); SQL | ./duckdb

Outcomes:
  BOTH_ERR         - both error, same error category → OK (context-dependent failure)
  ERR_DIFF         - both error, different error category → potential parser divergence
  OK               - both succeed, same output → OK
  ONE_ERR          - one succeeds, other errors → potential regression
  SILENT           - both succeed but different output → potential silent bug
  NON_DETERMINISTIC- default parser produces different output on two runs → skipped
  TIMEOUT          - exceeded per-statement time limit

Usage:
  python3 peg_suite_test.py                        # full suite (~36k stmts)
  python3 peg_suite_test.py --limit 500            # quick smoke test
  python3 peg_suite_test.py --workers 16           # more parallelism
  python3 peg_suite_test.py test/sql/window        # focus on a subdirectory
  python3 peg_suite_test.py --only-interesting     # print only ONE_ERR and SILENT
  python3 peg_suite_test.py --no-cleanup           # keep temp dir for debugging

Prerequisites:
  - Build release: cmake --build build/release --target shell autocomplete_loadable_extension
  - Paths below (DB, EXT) must point to the built binaries; edit if your layout differs.

Interpreting results (from the 2026-03 baseline run on 36,270 statements):
  OK               ~12 000  — both parsers agree
  BOTH_ERR         ~23 500  — both fail with same error; fine, need context (tables, extensions, …)
  ERR_DIFF              ?  — both fail but error messages differ; check with --only-interesting
  ONE_ERR               17  — real divergences; see test/sql/peg_parser/transformer/
  SILENT               700  — cosmetic column-name differences only (type alias display),
                              not semantic bugs; verify with --only-interesting before filing
  NON_DETERMINISTIC    ~50  — random()/uuid()/now() queries, correctly skipped
  TIMEOUT                3  — queries that exceed the 5 s per-statement limit
"""

import argparse
import atexit
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import uuid as _uuid_module
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DB = "/Users/danieltenwolde/git/duckdb/build/release/duckdb"
EXT = "/Users/danieltenwolde/git/duckdb/build/release/extension/autocomplete/autocomplete.duckdb_extension"
TIMEOUT = 5  # seconds per query

# Set once in main() before threads start; used by substitute_placeholders.
_TEMP_DIR: str = ""

# ── Regex constants ────────────────────────────────────────────────────────────

# Directives that introduce a SQL block in sqllogictest format.
# Note: foreach/loop are handled separately as loop directives, NOT as SQL starters.
SQL_STARTERS = re.compile(r"^(statement\s+(ok|error|maybe)|query\s+\S)", re.IGNORECASE)

# Lines that terminate SQL block collection.
SKIP_LINE = re.compile(
    r"^(#|require\b|mode\b|loop\b|endloop\b|sleep\b|halt\b|"
    r"skipif\b|onlyif\b|load\b|restart\b|reconnect\b|"
    r"concurrentloop\b|concurrent\b|endconcurrent\b|"
    r"statement\s+(ok|error|maybe)|query\s+\S|foreach\b)",
    re.IGNORECASE,
)

# Remaining ${varname} after all substitution — these statements are dropped.
UNRESOLVED_VAR = re.compile(r"\$\{[^}]+\}")

# Functions/keywords that make a query non-deterministic.
_NON_DET_FUNCS = re.compile(
    r"\b(random|uuid|gen_random_uuid|nextval|transaction_timestamp|setseed|epoch_ms)\s*\(",
    re.IGNORECASE,
)
_NON_DET_KEYWORDS = re.compile(
    r"\b(current_timestamp|current_date|current_time|now|today|yesterday)\b",
    re.IGNORECASE,
)


def _is_non_deterministic(sql: str) -> bool:
    return bool(_NON_DET_FUNCS.search(sql) or _NON_DET_KEYWORDS.search(sql))


# ── Placeholder substitution ───────────────────────────────────────────────────


def substitute_placeholders(sql: str, test_file_rel: str) -> str:
    """Replace all known sqllogictest placeholders, matching TestConfiguration::ProcessPath."""
    sql = sql.replace("{TEST_DIR}", _TEMP_DIR)
    sql = sql.replace("__TEST_DIR__", _TEMP_DIR)
    sql = sql.replace("{TEMP_DIR}", _TEMP_DIR)
    sql = sql.replace("{UUID}", str(_uuid_module.uuid4()))
    sql = sql.replace("{TEST_NAME}", test_file_rel)
    sql = sql.replace("{BASE_TEST_NAME}", test_file_rel.replace("/", "_"))
    sql = sql.replace("__WORKING_DIRECTORY__", os.getcwd())
    return sql


# ── foreach/loop expansion ─────────────────────────────────────────────────────

_SIGNED = ["tinyint", "smallint", "integer", "bigint", "hugeint"]
_UNSIGNED = ["utinyint", "usmallint", "uinteger", "ubigint", "uhugeint"]
_NUM_EXTRA = ["float", "double"]
_ALL_EXTRA = ["bool", "interval", "varchar"]

# Port of SQLLogicTestRunner::ForEachTokenReplace type-collection shortcuts.
_FOREACH_EXPANSIONS: dict[str, list[str]] = {
    "<signed>": _SIGNED,
    "<unsigned>": _UNSIGNED,
    "<integral>": _SIGNED + _UNSIGNED,
    "<numeric>": _SIGNED + _UNSIGNED + _NUM_EXTRA,
    "<alltypes>": _SIGNED + _UNSIGNED + _NUM_EXTRA + _ALL_EXTRA,
    "<compression>": [
        "none",
        "uncompressed",
        "rle",
        "bitpacking",
        "dictionary",
        "fsst",
        "dict_fsst",
        "alp",
        "alprd",
    ],
    "<all_types_columns>": [
        "bool",
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "hugeint",
        "uhugeint",
        "utinyint",
        "usmallint",
        "uint",
        "ubigint",
        "date",
        "time",
        "timestamp",
        "timestamp_s",
        "timestamp_ms",
        "timestamp_ns",
        "time_tz",
        "timestamp_tz",
        "float",
        "double",
        "dec_4_1",
        "dec_9_4",
        "dec_18_6",
        "dec38_10",
        "uuid",
        "interval",
        "varchar",
        "blob",
        "bit",
        "small_enum",
        "medium_enum",
        "large_enum",
        "int_array",
        "double_array",
        "date_array",
        "timestamp_array",
        "timestamptz_array",
        "varchar_array",
        "nested_int_array",
        "struct",
        "struct_of_arrays",
        "array_of_structs",
        "map",
        "union",
        "fixed_int_array",
        "fixed_varchar_array",
        "fixed_nested_int_array",
        "fixed_nested_varchar_array",
        "fixed_struct_array",
        "struct_of_fixed_array",
        "fixed_array_of_int_list",
        "list_of_fixed_int_array",
    ],
}


def _foreach_token_replace(token: str, result: list[str]) -> bool:
    """
    Port of C++ ForEachTokenReplace.
    Modifies result in-place. Returns True if token was a special collection keyword.
    If False, caller should append the literal token to result.
    """
    low = token.strip().lower()
    if low.startswith("!"):
        target = token[1:]
        if target not in result:
            return False  # not found → push literal "!token"
        result.remove(target)
        return True
    expansion = _FOREACH_EXPANSIONS.get(low)
    if expansion is not None:
        result.extend(expansion)
        return True
    return False


def expand_foreach_values(raw_tokens: list[str]) -> list[str]:
    """Expand a foreach token list, resolving <integral> etc. type shortcuts."""
    result: list[str] = []
    for tok in raw_tokens:
        if not _foreach_token_replace(tok, result):
            result.append(tok)
    return result


# ── SQL extraction ─────────────────────────────────────────────────────────────


def _apply_var_bindings(line: str, bindings: dict[str, str]) -> str:
    for varname, val in bindings.items():
        line = line.replace("${" + varname + "}", val)
    return line


def _find_matching_endloop(lines: list[str], start: int) -> tuple[int, int]:
    """
    Find the endloop that matches a foreach/loop whose body starts at `start`.
    Returns (body_end_exclusive, index_after_endloop).
    """
    depth = 1
    i = start
    while i < len(lines):
        s = lines[i].strip().lower()
        if re.match(r"^(foreach|loop)\b", s):
            depth += 1
        elif re.match(r"^endloop\b", s):
            depth -= 1
            if depth == 0:
                return i, i + 1
        i += 1
    # No matching endloop — treat entire remainder as body
    return len(lines), len(lines)


def _extract_from_lines(
    lines: list[str],
    test_file_rel: str,
    var_bindings: dict[str, str],
    start: int = 0,
    end: int | None = None,
):
    """Core recursive SQL extractor. Handles foreach/loop expansion."""
    if end is None:
        end = len(lines)
    i = start
    while i < end:
        raw_line = lines[i]
        line = _apply_var_bindings(raw_line, var_bindings)
        stripped = line.strip()

        # ── foreach directive ──────────────────────────────────────────────
        if re.match(r"^foreach\b", stripped, re.IGNORECASE):
            tokens = stripped.split()
            if len(tokens) >= 2:
                varname = tokens[1]
                # Skip multi-variable (comma-syntax) foreach — too complex, rarely used
                if "," not in varname:
                    raw_values = tokens[2:]
                    values = expand_foreach_values(raw_values)
                    body_start = i + 1
                    body_end, next_i = _find_matching_endloop(lines, body_start)
                    for val in values:
                        new_bindings = {**var_bindings, varname: val}
                        yield from _extract_from_lines(lines, test_file_rel, new_bindings, body_start, body_end)
                    i = next_i
                    continue
            i += 1
            continue

        # ── loop directive (numeric range) ─────────────────────────────────
        if re.match(r"^loop\b", stripped, re.IGNORECASE):
            tokens = stripped.split()
            if len(tokens) >= 4:
                varname = tokens[1]
                try:
                    lo, hi = int(tokens[2]), int(tokens[3])
                    body_start = i + 1
                    body_end, next_i = _find_matching_endloop(lines, body_start)
                    for val in range(lo, hi):
                        new_bindings = {**var_bindings, varname: str(val)}
                        yield from _extract_from_lines(lines, test_file_rel, new_bindings, body_start, body_end)
                    i = next_i
                    continue
                except (ValueError, IndexError):
                    pass
            i += 1
            continue

        # ── endloop (should not be reached in well-formed input) ───────────
        if re.match(r"^endloop\b", stripped, re.IGNORECASE):
            i += 1
            continue

        # ── SQL statement/query directive ──────────────────────────────────
        if SQL_STARTERS.match(stripped):
            sql_lines = []
            i += 1
            while i < end:
                raw_l = lines[i]
                l = _apply_var_bindings(raw_l, var_bindings)
                if not l.strip() or l.startswith("----"):
                    break
                if SKIP_LINE.match(l.strip()):
                    break
                sql_lines.append(l)
                i += 1
            sql = "\n".join(sql_lines).strip()
            if sql and len(sql) < 8000:
                sql = substitute_placeholders(sql, test_file_rel)
                if not sql.rstrip().endswith(";"):
                    sql += ";"
                # Drop statements with unresolved ${...} (e.g., from outer scopes)
                if not UNRESOLVED_VAR.search(sql):
                    yield sql
            continue

        i += 1


def extract_sql_blocks(path: Path, test_file_rel: str):
    """Yield individual SQL statements from a sqllogictest file."""
    try:
        text = path.read_text(errors="replace")
    except Exception:
        return
    yield from _extract_from_lines(text.splitlines(), test_file_rel, {})


# ── PEG preamble stripping ─────────────────────────────────────────────────────


def strip_peg_preamble(output: str) -> str:
    """
    Remove the single result-set box produced by LOAD autocomplete extension.
    (CALL enable_peg_parser() produces no visible output.)

    The preamble box is always the narrow 9-column success box:
      ┌─────────┐
      │ success │ │ boolean │ └─────────┘   0 rows
    followed immediately (no blank line) by the actual query result.
    """
    lines = output.splitlines()
    in_preamble = False
    past = False
    result = []
    for line in lines:
        # Detect the narrow success box that LOAD emits
        if not past and not in_preamble and "┌─────────┐" in line:
            in_preamble = True
            continue
        if in_preamble:
            if "0 rows" in line:
                in_preamble = False
                past = True  # everything after is real output
            continue
        result.append(line)
    return "\n".join(result).strip()


# ── Output formatting ──────────────────────────────────────────────────────────


def _fmt_output(output: str, max_lines: int = 8, indent: str = "    ") -> str:
    """Format query output for display: indented, truncated to max_lines."""
    lines = output.splitlines()
    shown = lines[:max_lines]
    body = "\n".join(indent + l for l in shown)
    if len(lines) > max_lines:
        body += f"\n{indent}... ({len(lines) - max_lines} more lines)"
    return body


def _print_diff(src: str, sql: str, d_out: str, p_out: str) -> None:
    print(f"\n  ┌─ [{src}]")
    print(f"  │  SQL: {textwrap.shorten(sql, 120)}")
    print(f"  │  DEFAULT:")
    print(_fmt_output(d_out, indent="  │    "))
    print(f"  │  PEG:")
    print(_fmt_output(p_out, indent="  │    "))


# ── Error comparison ───────────────────────────────────────────────────────────


def _error_prefix(output: str) -> str:
    """Extract the error category — text before the first ':' on the first line.

    DuckDB errors look like:  "Parser error: syntax error at or near ..."
                               "Binder Error: table ... does not exist"
    Comparing only this prefix avoids false ERR_DIFF from cosmetically different
    but semantically identical messages.
    """
    first_line = output.splitlines()[0] if output else ""
    return first_line.split(":")[0].strip().lower()


# ── Query execution ────────────────────────────────────────────────────────────


def run_sql(sql: str, use_peg: bool = False) -> tuple[int, str]:
    """Run SQL through DuckDB. Returns (exit_code, output_text)."""
    if use_peg:
        full = f"LOAD '{EXT}';\nCALL enable_peg_parser();\n{sql}"
    else:
        full = sql  # no preamble for the default parser
    try:
        r = subprocess.run([DB], input=full, capture_output=True, text=True, timeout=TIMEOUT)
        out = (r.stdout + r.stderr).strip()
        if use_peg:
            out = strip_peg_preamble(out)
        return r.returncode, out
    except subprocess.TimeoutExpired:
        return -999, "TIMEOUT"
    except Exception as e:
        return -1, str(e)


def classify(sql: str) -> tuple[str, str, str, str]:
    """
    Classify a SQL statement by comparing default vs PEG parser outcomes.

    Non-determinism check: if the query contains non-deterministic functions/keywords,
    run the default parser twice. If the two runs differ, the query is non-deterministic
    and cannot be used for parser comparison.
    """
    d1_exit, d1_out = run_sql(sql, use_peg=False)

    if d1_exit == -999:
        return "TIMEOUT", sql, d1_out, ""

    # Only pay the cost of a second default run when the query looks non-deterministic.
    if _is_non_deterministic(sql):
        d2_exit, d2_out = run_sql(sql, use_peg=False)
        if d2_exit == -999:
            return "TIMEOUT", sql, d1_out, ""
        if d1_exit != d2_exit or d1_out != d2_out:
            return "NON_DETERMINISTIC", sql, d1_out, d2_out

    p_exit, p_out = run_sql(sql, use_peg=True)

    if p_exit == -999:
        return "TIMEOUT", sql, d1_out, p_out

    both_ok = d1_exit == 0 and p_exit == 0
    both_err = d1_exit != 0 and p_exit != 0

    if both_err:
        if _error_prefix(d1_out) == _error_prefix(p_out):
            return "BOTH_ERR", sql, d1_out, p_out
        return "ERR_DIFF", sql, d1_out, p_out
    if both_ok:
        if d1_out == p_out:
            return "OK", sql, d1_out, p_out
        return "SILENT", sql, d1_out, p_out
    return "ONE_ERR", sql, d1_out, p_out


# ── Main ───────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Differential PEG parser test against the DuckDB test suite.")
    parser.add_argument("test_dir", nargs="?", default="/Users/danieltenwolde/git/duckdb/test")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--limit", type=int, default=0, help="Max statements to test (0=all)")
    parser.add_argument("--show-ok", action="store_true")
    parser.add_argument(
        "--only-interesting",
        action="store_true",
        help="Only print ONE_ERR and SILENT results",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Keep temp dir after run (for debugging)",
    )
    args = parser.parse_args()

    # Set up temp directory before any extraction (substitute_placeholders reads it).
    global _TEMP_DIR
    _TEMP_DIR = tempfile.mkdtemp(prefix="peg_suite_")
    print(f"Temp dir: {_TEMP_DIR}", flush=True)
    if not args.no_cleanup:
        atexit.register(shutil.rmtree, _TEMP_DIR, ignore_errors=True)

    test_root = Path(args.test_dir)
    test_files = sorted(test_root.rglob("*.test"))
    print(f"Found {len(test_files)} test files", flush=True)

    # Collect all SQL blocks, deduplicating by normalized content.
    all_sql: list[tuple[str, str]] = []
    seen: set[str] = set()
    for tf in test_files:
        rel = str(tf.relative_to(test_root))
        for sql in extract_sql_blocks(tf, rel):
            key = sql.lower().strip()
            if key in seen:
                continue
            seen.add(key)
            all_sql.append((rel, sql))
            if args.limit and len(all_sql) >= args.limit:
                break
        if args.limit and len(all_sql) >= args.limit:
            break

    print(f"Extracted {len(all_sql)} unique SQL statements", flush=True)
    print(f"Running with {args.workers} workers ...\n", flush=True)

    counts: dict[str, int] = defaultdict(int)
    one_err: list[tuple] = []
    silent: list[tuple] = []
    err_diff: list[tuple] = []
    timeout: list[tuple] = []

    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(classify, sql): (src, sql) for src, sql in all_sql}
        for fut in as_completed(futures):
            src, sql = futures[fut]
            try:
                label, sql, d_out, p_out = fut.result()
            except Exception as e:
                label, d_out, p_out = "ERROR", "", str(e)
            counts[label] += 1
            done += 1
            if done % 500 == 0:
                print(
                    f"  {done}/{len(all_sql)} done  "
                    f"[OK={counts['OK']} BOTH_ERR={counts['BOTH_ERR']} "
                    f"ERR_DIFF={counts['ERR_DIFF']} ONE_ERR={counts['ONE_ERR']} "
                    f"SILENT={counts['SILENT']} NON_DET={counts['NON_DETERMINISTIC']}]",
                    flush=True,
                )
            if label == "ONE_ERR":
                one_err.append((src, sql, d_out, p_out))
            elif label == "SILENT":
                silent.append((src, sql, d_out, p_out))
            elif label == "ERR_DIFF":
                err_diff.append((src, sql, d_out, p_out))
            elif label == "TIMEOUT":
                timeout.append((src, sql))

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for k in ["OK", "BOTH_ERR", "ERR_DIFF", "ONE_ERR", "SILENT", "NON_DETERMINISTIC", "TIMEOUT", "ERROR"]:
        print(f"  {k:18s}: {counts[k]}")

    if silent:
        print(f"\n{'=' * 70}")
        print(f"SILENT DIFFERENCES ({len(silent)}) — potential bugs:")
        print(f"{'=' * 70}")
        for src, sql, d_out, p_out in silent:
            _print_diff(src, sql, d_out, p_out)

    if err_diff:
        print(f"\n{'=' * 70}")
        print(f"ERROR DIFFERENCES ({len(err_diff)}) — both fail but different error category:")
        print(f"{'=' * 70}")
        for src, sql, d_out, p_out in err_diff[:100]:
            _print_diff(src, sql, d_out, p_out)
        if len(err_diff) > 100:
            print(f"\n  ... and {len(err_diff) - 100} more")

    if one_err:
        print(f"\n{'=' * 70}")
        print(f"ONE-SIDED ERRORS ({len(one_err)}) — regressions or improvements:")
        print(f"{'=' * 70}")
        for src, sql, d_out, p_out in one_err[:100]:
            _print_diff(src, sql, d_out, p_out)
        if len(one_err) > 100:
            print(f"\n  ... and {len(one_err) - 100} more")

    if timeout:
        print(f"\n{'=' * 70}")
        print(f"TIMEOUTS ({len(timeout)}):")
        print(f"{'=' * 70}")
        for src, sql in timeout:
            print(f"  [{src}] {textwrap.shorten(sql, 100)}")


if __name__ == "__main__":
    main()
