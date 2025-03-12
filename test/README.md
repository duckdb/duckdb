This is the SQL and C++ DuckDB Unittests

## Environment Variable Flags

Some of the tests require a special environment flag to be set so they can properly run.

### Parallel CSV Reader
The tests located in `test/parallel_csv/test_parallel_csv.cpp` run the parallel CSV reader over multiple configurations of threads
and buffer sizes.

### ADBC
The ADBC tests are in `test/api/adbc/test_adbc.cpp`. To run them the DuckDB library path must be provided in the `DUCKDB_INSTALL_LIB` variable.
