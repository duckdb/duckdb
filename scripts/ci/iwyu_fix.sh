#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${1:-python3}"
IWYU_DIR="build/iwyu"
IWYU_OUT="${IWYU_DIR}/iwyu.out"
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

"${PYTHON_BIN}" "${FIX_SCRIPT}" --nocomments < "${IWYU_OUT}"
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
