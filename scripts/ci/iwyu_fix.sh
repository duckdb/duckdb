#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${1:-python3}"
IWYU_DIR="build/iwyu"
IWYU_OUT="${IWYU_DIR}/iwyu.out"
IWYU_OUT_FILTERED="${IWYU_DIR}/iwyu.core_functions.filtered.out"
IWYU_PATCH="${IWYU_DIR}/iwyu.patch"
FIX_SCRIPT="${IWYU_DIR}/fix_includes.py"
FIX_SCRIPT_URL="https://raw.githubusercontent.com/include-what-you-use/include-what-you-use/9175c4eeb6084495a6945a9f33de1fc16ec0384e/fix_includes.py"

if [[ ! -f "${IWYU_OUT}" ]]; then
	echo "${IWYU_OUT} not found. Run 'make iwyu' first."
	exit 1
fi

set -x

mkdir -p "${IWYU_DIR}"
curl --retry 5 -fsSL "${FIX_SCRIPT_URL}" -o "${FIX_SCRIPT}"

# Restrict IWYU auto-fixes to extension/core_functions and never add angle includes.
awk '
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
		keep_file = (index(active_file, "extension/core_functions/") == 1)
		in_add_block = 1
		pending_add_header = keep_file ? $0 : ""
		next
	}
	if ($0 ~ / should remove these lines:$/) {
		flush_add_block()
		active_file = $0
		sub(/ should remove these lines:$/, "", active_file)
		keep_file = (index(active_file, "extension/core_functions/") == 1)
		in_add_block = 0
		if (keep_file) {
			print $0
		}
		next
	}
	if ($0 ~ /^The full include-list for .*:$/) {
		flush_add_block()
		active_file = $0
		sub(/^The full include-list for /, "", active_file)
		sub(/:$/, "", active_file)
		keep_file = (index(active_file, "extension/core_functions/") == 1)
		in_add_block = 0
		if (keep_file) {
			print $0
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
