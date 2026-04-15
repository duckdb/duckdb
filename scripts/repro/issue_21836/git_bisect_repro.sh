#!/usr/bin/env bash
# Helper for git bisect: exit 0 = good, 1 = bad (OOM or error).
# Usage (from repo root):
#   git bisect start <bad> <good>
#   git bisect run ./scripts/repro/issue_21836/git_bisect_repro.sh
#
# Requires a duckdb binary: prefers DUCKDB_BIN, then build/release/duckdb,
# then build/debug/duckdb.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DUCKDB="${DUCKDB_BIN:-}"
if [[ -z "${DUCKDB}" ]]; then
	if [[ -x "${ROOT}/build/release/duckdb" ]]; then
		DUCKDB="${ROOT}/build/release/duckdb"
	elif [[ -x "${ROOT}/build/debug/duckdb" ]]; then
		DUCKDB="${ROOT}/build/debug/duckdb"
	else
		echo "No duckdb binary found; set DUCKDB_BIN" >&2
		exit 125
	fi
fi

if "${DUCKDB}" < "${ROOT}/scripts/repro/issue_21836/repro.sql" >/dev/null 2>&1; then
	exit 0
fi
exit 1
