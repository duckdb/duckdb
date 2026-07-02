This is the SQL and C++ DuckDB Unittests

Typical entrypoints:
- POSIX builds: `build/release/test/run`, `build/reldebug/test/run`, `build/debug/test/run`
- Windows native layout: `build/release/test/run.py` or `build/release/test/run.bat`

## Test Contract

Within SQL tests, the test runner (unittest) guarantees that these environment variables will be set. Many can be overridden when needed.

| Env Var          | Default Value              | Overrides                                                  | Notes                                                                              |
|------------------|----------------------------|-------------------------------------------------------------|------------------------------------------------------------------------------------|
| `TEST_NAME` | e.g. `test/path/filename.test` | N/A |                                                                                    |
| `TEST_NAME__NO_SLASH` | e.g. `test_path_filename.test` | N/A | is always `TEST_NAME s@/@_@g`; same value as `TEST_ID`                             |
| `TEST_ID` | `= TEST_NAME__NO_SLASH` | N/A | per-test identity; the `[TEST_ID]` path level under `TEMP_DIR_BASE`                 |
| `TEST_UUID` | random UUID (as string) | N/A | defined per invocation                                                             |
| `RUN_ID` | generated (`<ts>--<mnemonic>`) | `unittest --run-id <id>` | per-run identity; the `[RUN_ID]` path level; shared across a run's tests            |
| `WORKING_DIR`    | e.g., `/Users/me/src/duckdb`     | `unittest --test-dir <path>`                                | default: wherever duckdb was sourced and built <br/> AKA: `{WORKING_DIRECTORY}`    |
| `BUILD_DIR`      | `{WORKING_DIR}/build/release` | N/A                                                         | read-only; derived from unittest path <br/> AKA: `{BUILD_DIRECTORY}`               |
| `DATA_DIR`  | `{WORKING_DIR}/data`        | `DATA_DIR=mydata unittest ...`<br/>also test configs, `test_env` specification | Should provide a copy of `duckdb/data/**`, use to test AWS, Azure, other VFS reads |
| `TEMP_DIR_BASE` | `{WORKING_DIR}/duckdb_unittest_tempdir` | `unittest --temp-dir-base <path>` | root of the temp-dir tree; may be local or remote (e.g. `s3://`)                    |
| `TEMP_DIR` | `{TEMP_DIR_BASE}/[RUN_ID]/[TEST_ID]` | `--temp-dir-run-id {on,off}` / `--temp-dir-test-id {on,off}` (include/omit each level); `--temp-dir-{create,destroy}` govern lifecycle | the per-test temp dir; use to test VFS writes <br/> AKA: `{TEST_DIR}`, deprecated `__TEST_DIR__` |
| `TEMP_DIR_ABSOLUTE` | absolute path to `{TEMP_DIR}` | N/A | use when a test requires an absolute path, e.g. `file://` URIs                     |
| `CATALOG_DIR` | `{TEMP_DIR}/{TEST_UUID}` | N/A | a per-test catalog dir; NOT guaranteed to exist (create on demand)                 |

Some tests require a particular environment variables to be set so they can properly run, usually this can be seen via `require-env VAR` in the test.

### Parallel CSV Reader

The tests located in `test/parallel_csv/test_parallel_csv.cpp` run the parallel CSV reader over multiple configurations of threads
and buffer sizes.

### ADBC

The ADBC tests are in `test/api/adbc/test_adbc.cpp`. To run them the DuckDB library path must be provided in the `DUCKDB_INSTALL_LIB` variable.
