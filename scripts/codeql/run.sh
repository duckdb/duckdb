#!/usr/bin/env bash
set -eu -o pipefail

# arch -x86_64
# codeql database analyze build/codeql/db-cpp \
#   "$CODEQL_HOME/cpp/ql/src/codeql-suites/cpp-code-scanning.qls" \
#   --format=sarifv2.1.0 \
#   --output=scripts/codeql/out/results.sarif

PACK_DIR="scripts/codeql"
OUT_DIR="$PACK_DIR/out"
PACK_LOCK_FILE="$PACK_DIR/codeql-pack.lock.yml"
PACK_MANIFEST_FILE="$PACK_DIR/qlpack.yml"
PACK_INSTALL_STAMP="$OUT_DIR/.codeql-pack-install.stamp"
SARIF_OUTPUT_FILE="$OUT_DIR/results.sarif"
LOCK_OUTPUT_FILE="$OUT_DIR/codeql-pack.lock.yml"
DB_PATH="${1:-build/codeql/db-cpp}"

mkdir -p "$OUT_DIR"

if [[ ! -f "$PACK_INSTALL_STAMP" || "$PACK_LOCK_FILE" -nt "$PACK_INSTALL_STAMP" || "$PACK_MANIFEST_FILE" -nt "$PACK_INSTALL_STAMP" ]]; then
  (cd "$PACK_DIR"; codeql pack install)
  touch "$PACK_INSTALL_STAMP"
fi

set -x

# arch -x86_64
codeql database analyze "$DB_PATH" \
  scripts/codeql/indirect-throw-in-destructor.ql \
  --verbose \
  --threads=0 \
  --format=sarifv2.1.0 \
  --output="$SARIF_OUTPUT_FILE"

BQRS_PATH="$DB_PATH/results/duckdb/custom-cpp-queries/indirect-throw-in-destructor.bqrs"
codeql bqrs decode --format=json -- "$BQRS_PATH" > "$OUT_DIR/results.json"
cp "$PACK_LOCK_FILE" "$LOCK_OUTPUT_FILE"
