This is the SQL and C++ DuckDB Unittests

## Test Contract

Within SQL tests, the test runner (unittest) guarantees that these environment variables will be set. Many can be overridden when needed.

| Env Var          | Default Value              | Overrides                                                  | Notes                                              |
|------------------|----------------------------|-------------------------------------------------------------|---------------------------------------------------|
| `WORKING_DIR`    | `/Users/me/src/duckdb`     | `unittest --test-dir <path>`                                | default: wherever duckdb was sourced and built <br/> AKA: `__WORKING_DIRECTORY__`    |
| `BUILD_DIR`      | `${WORKING_DIR}/build/release` | N/A                                                         | read-only; derived from unittest path <br/> AKA: `__BUILD_DIRECTORY__`             |
| `TEST_DATA_LOC`  | `${WORKING_DIR}/data`        | `TEST_DATA_LOC=mydata unittest ...`<br/>also test configs, `test_env` specification | Should provide a copy of `duckdb/data/**`, use to test AWS, Azure, other VFS reads          |
| `TEST_TEMP_LOC` | `${WORKING_DIR}/duckdb_unittest_tempdir/$PID`          | via `unittest --test-temp-dir` (retains dir after test)      | use to test VFS writes <br/> AKA: `__TEST_DIR__`                            |
| `TEST_CATALOG_LOC` | `${TEST_SCRATCH_LOC}/${UUID}.db`                         |  `unittest --test-config ...` or `unittest --test-env ...`                                                         |                                                   |
| `TEST_NAME` | e.g. `test/path/filename.test` | N/A | |
| `TEST_NAME_NO_SLASH` | e.g. `test_path_filename.test` | N/A | is always `$TEST_NAME s@/@_@g`|

Some of the tests require a special environment flag to be set so they can properly run.

### Parallel CSV Reader

The tests located in `test/parallel_csv/test_parallel_csv.cpp` run the parallel CSV reader over multiple configurations of threads
and buffer sizes.

### ADBC

The ADBC tests are in `test/api/adbc/test_adbc.cpp`. To run them the DuckDB library path must be provided in the `DUCKDB_INSTALL_LIB` variable.
