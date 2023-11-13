This is the SQL and C++ DuckDB Unittests

## Environment Variable Flags

Some of the tests require a special environment flag to be set so they can properly run.

### Parallel CSV Reader
The tests located in `test/parallel_csv/test_parallel_csv.cpp` run the parallel CSV reader over multiple configurations of threads
and buffer sizes.
To run them `DUCKDB_RUN_PARALLEL_CSV_TESTS` must be set. This is done because these tests are quite slow and should only be executed
on release builds with no debugging options set.

### ADBC
The ADBC tests are in `test/api/adbc/test_adbc.cpp`. To run them the DuckDB library path must be provided in the `DUCKDB_INSTALL_LIB` variable.
