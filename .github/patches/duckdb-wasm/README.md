# WASM patches
Patches in this directory are used to smoothen the process of introducing changes to DuckDB that break compatibility with
the current duckdb-wasm version as is pinned in the `.github/workflows/NightlyTests.yml` workflow.

# Workflow
Imagine a change to DuckDB is introduced that breaks compatibility with current WASM. The
workflow for this is as follows:

### PR #1: breaking change to DuckDB
- Commit breaking change to DuckDB
- Fix breakage in duckdb-wasm, producing a patch with fix (be wary of already existing patches)
- Commit patch in `.github/patches/duckdb-wasm/*.patch` using a descriptive name

### PR #2: patch to duckdb-wasm
- Apply (all) the patch(es) in `.github/patches/duckdb-wasm/*.patch` to duckdb-wasm.

### PR #3: update extension X in DuckDB
- Remove patches in `.github/patches/duckdb-wasm/*.patch`
- Update hash of duckdb-wasm in `.github/workflows/NightlyTests.yml`