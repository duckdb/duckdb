#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${1:-python3}"
IWYU_DIR="build/iwyu"
IWYU_OUT="${IWYU_DIR}/iwyu.out"
IWYU_OUT_FILTERED="${IWYU_DIR}/iwyu.core_functions.filtered.out"
IWYU_PATCH="${IWYU_DIR}/iwyu.patch"
FIX_SCRIPT="${IWYU_DIR}/fix_includes.py"
FIX_SCRIPT_URL="https://raw.githubusercontent.com/include-what-you-use/include-what-you-use/9175c4eeb6084495a6945a9f33de1fc16ec0384e/fix_includes.py"
# Add more repo-relative path prefixes here to broaden IWYU fix scope.
IWYU_PATH_FILTERS=("extension/core_functions/")

if [[ ! -f "${IWYU_OUT}" ]]; then
	echo "${IWYU_OUT} not found. Run 'make iwyu' first."
	exit 1
fi

set -x

mkdir -p "${IWYU_DIR}"
curl --retry 5 -fsSL "${FIX_SCRIPT_URL}" -o "${FIX_SCRIPT}"

AWK_FILTER_LIST=""
for filter in "${IWYU_PATH_FILTERS[@]}"; do
	if [[ -n "${AWK_FILTER_LIST}" ]]; then
		AWK_FILTER_LIST+=$'\n'
	fi
	AWK_FILTER_LIST+="${filter}"
done

# Restrict IWYU auto-fixes to extension/core_functions and never add angle includes.
awk -v filters_raw="${AWK_FILTER_LIST}" -v pwd_prefix="${PWD}/" '
function normalize_path(path) {
	sub(/^\/home\/runner\/work\/duckdb\/duckdb\//, "", path)
	if (index(path, pwd_prefix) == 1) {
		path = substr(path, length(pwd_prefix) + 1)
	}
	return path
}
function matches_filter(path,  i, norm_path) {
	norm_path = normalize_path(path)
	for (i = 1; i <= filter_count; i++) {
		if (index(norm_path, filters[i]) == 1) {
			return 1
		}
	}
	return 0
}
function flush_add_block(  i) {
	if (!pending_add_header) {
		return
	}
	if (pending_add_count > 0) {
		print pending_add_header
		for (i = 1; i <= pending_add_count; i++) {
			print pending_add_lines[i]
		}
	}
	pending_add_header = ""
	pending_add_count = 0
}
BEGIN {
	filter_count = split(filters_raw, filters, /\n/)
	in_add_block = 0
	pending_add_header = ""
	pending_add_count = 0
	active_file = ""
	keep_file = 0
}
{
	if ($0 ~ / should add these lines:$/) {
		flush_add_block()
		active_file = $0
		sub(/ should add these lines:$/, "", active_file)
		active_file = normalize_path(active_file)
		keep_file = matches_filter(active_file)
		in_add_block = 1
		pending_add_header = keep_file ? active_file " should add these lines:" : ""
		next
	}
	if ($0 ~ / should remove these lines:$/) {
		flush_add_block()
		active_file = $0
		sub(/ should remove these lines:$/, "", active_file)
		active_file = normalize_path(active_file)
		keep_file = matches_filter(active_file)
		in_add_block = 0
		if (keep_file) {
			print active_file " should remove these lines:"
		}
		next
	}
	if ($0 ~ /^The full include-list for .*:$/) {
		flush_add_block()
		active_file = $0
		sub(/^The full include-list for /, "", active_file)
		sub(/:$/, "", active_file)
		active_file = normalize_path(active_file)
		keep_file = matches_filter(active_file)
		in_add_block = 0
		if (keep_file) {
			print "The full include-list for " active_file ":"
		}
		next
	}
	if (in_add_block) {
		if (!keep_file) {
			next
		}
		# Drop all angle-bracket include additions (e.g., stdlib headers).
		if ($0 ~ /^#include[[:space:]]+<[^>]+>/) {
			next
		}
		pending_add_count++
		pending_add_lines[pending_add_count] = $0
		next
	}
	if (keep_file) {
		print $0
	}
}
END {
	flush_add_block()
}
' "${IWYU_OUT}" > "${IWYU_OUT_FILTERED}"

"${PYTHON_BIN}" "${FIX_SCRIPT}" --nocomments < "${IWYU_OUT_FILTERED}"
git diff > "${IWYU_PATCH}"

if ! git diff --exit-code; then
	cat <<'EOF'

==================== IWYU FIX GUIDE ====================

1) Missing include (removed but needed)
   Add it back with:
     #include <some/header>  // IWYU pragma: keep

2) Unnecessary include (added but not needed)
   Delete the #include

3) Incorrect forward declaration
   Fix manually (add/remove/adjust)

4) Private header suggested (e.g. <bits/...>)
   In the private header, add:
     // IWYU pragma: private, include "public/header.h"

Notes:
- 'IWYU pragma: keep' is case-sensitive
- Prefer public headers (e.g. <vector>) over private ones

========================================================

EOF
	exit 1
fi
