#!/bin/bash

echo -e "[ODBC]\nTrace = yes\nTraceFile = /tmp/odbctrace\n\n[DuckDB Driver]\nDriver = "$(pwd)"/build/debug/tools/odbc/libduckdb_odbc.so" > ~/.odbcinst.ini
echo -e "[DuckDB]\nDriver = DuckDB Driver\nDatabase=:memory:\n" > ~/.odbc.ini

export PSQLODBC_TEST_DSN="DuckDB"

BASE_DUCKDB_DIR=$(pwd)
cd psqlodbc
export PSQLODBC_DIR=$(pwd)

# creating contrib_regression database used by some tests
./build/debug/reset-db < sampletables.sql
if [[ $? != 0 ]]; then
    cat /tmp/odbctrace
    exit 1
fi

# running supported tests
./build/debug/psql_odbc_test -f ${BASE_DUCKDB_DIR}/tools/odbc/test/psql_supported_tests
if [[ $? != 0 ]]; then
    cat /tmp/odbctrace
    exit 1
fi
