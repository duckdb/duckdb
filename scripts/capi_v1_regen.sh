#!/usr/bin/env bash
# Regenerates the V1 C API headers from api_spec/v1/, then formats the outputs.
# Invoked manually after editing YAML, or via make generate-files. Expects `capigen` on PATH.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

capigen c \
	--spec-dir api_spec/v1 \
	-o src/include/duckdb.h

capigen extension_header \
	--spec-dir api_spec/v1 \
	--template api_spec/v1/extension/duckdb_extension.h.in \
	--internal-out src/include/duckdb/main/capi/extension_api.hpp \
	-o src/include/duckdb_extension.h

python3 scripts/format.py src/include/duckdb.h --fix --noconfirm
python3 scripts/format.py src/include/duckdb_extension.h --fix --noconfirm
python3 scripts/format.py src/include/duckdb/main/capi/extension_api.hpp --fix --noconfirm
