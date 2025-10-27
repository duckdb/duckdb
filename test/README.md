This is the SQL and C++ DuckDB Unittests

## Test Contract

Within SQL tests, the test runner (unittest) guarantees that these environment variables will be set. Many can be overridden when needed.

| Env Var              | Default Value                           | CLI Override                                   | Notes                                                       |
| -------------------- | --------------------------------------- | ---------------------------------------------- | ----------------------------------------------------------- |
| `TEST_NAME`          | e.g. `test/path/filename.test`          | N/A                                            |                                                             |
| `TEST_NAME_NO_SLASH` | e.g. `test_path_filename.test`          | N/A                                            | is always `TEST_NAME s@/@_@g`                               |
| `TEST_UUID`          | random UUID (as string)                 | N/A                                            | per-invocation ID, not per-test                             |
| `WORKING_DIR`        | e.g., `/Users/me/src/duckdb`            | `unittest --test-dir <path>`                   | default: where you run unittest                             |
| `BUILD_DIR`          | `{WORKING_DIR}/build/release`           | N/A                                            | read-only; derived during build; AKA: `__BUILD_DIRECTORY__` |
| `DATA_DIR`           | `{WORKING_DIR}/data`                    | `--data-dir`                                   | Must have copy of `duckdb/data/**`                          |
| `TEMP_DIR`           | `{TEMP_DIR_BASE}/<process-id>`          | `--temp-dir`; (`--test-temp-dir` for local FS) | use to test VFS writes <br/> AKA: `__TEST_DIR__`            |
| `TEMP_DIR_BASE`      | `{WORKING_DIR}/duckdb_unittest_tempdir` | `--temp-dir-base`                              | use to test VFS writes <br/> AKA: `__TEST_DIR__`            |

Some tests require a particular environment variables to be set so they can properly run, usually this can be seen via `require-env VAR` in the test.

### Parallel CSV Reader

The tests located in `test/parallel_csv/test_parallel_csv.cpp` run the parallel CSV reader over multiple configurations of threads
and buffer sizes.

### ADBC

The ADBC tests are in `test/api/adbc/test_adbc.cpp`. To run them the DuckDB library path must be provided in the `DUCKDB_INSTALL_LIB` variable.
