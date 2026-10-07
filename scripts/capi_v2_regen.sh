#!/usr/bin/env bash
# Regenerates the V2 C API header and bridge stubs from api_spec/, then
# formats the regenerated files. Invoked manually after editing YAML and
# automatically by the matching pre-commit hook.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

uv run --project api_spec --group generate capigen c \
    --spec-dir api_spec/v2 \
    -o src/include/duckdb_v2.h

uv run --project api_spec --group generate capigen bridge \
    --spec-dir api_spec/v2 \
    --scan-dir src/main/capi/v2 \
    -o src/main/capi/v2/capi_v2_stubs.cpp

uv run --project api_spec --group generate capigen extension_header \
    --spec-dir api_spec/v2 \
    --template api_spec/v2/extension/duckdb_extension_v2.h.in \
    --internal-out src/include/duckdb/main/capi_v2/extension_api_v2.hpp \
    -o src/include/duckdb_extension_v2.h

# Format twice: clang-format is not idempotent on the generated
# `DUCKDB_V2_ERROR (*fn)(...)` struct members (the first pass line-breaks them,
# the second settles them). A single pass leaves output the format hook would
# rewrite, making pre-commit never converge.
uv run --project api_spec --group dev python scripts/format.py HEAD --fix --noconfirm --silent
uv run --project api_spec --group dev python scripts/format.py HEAD --fix --noconfirm --silent
