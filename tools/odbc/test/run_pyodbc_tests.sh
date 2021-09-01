#!/bin/bash

echo -e "[ODBC]\nTrace = yes\nTraceFile = /tmp/odbctrace\n\n[DuckDB Driver]\nDriver = "$(pwd)"/build/debug/tools/odbc/libduckdb_odbc.so" > ~/.odbcinst.ini
echo -e "[DuckDB]\nDriver = DuckDB Driver\nDatabase=:memory:\n" > ~/.odbc.ini

export ASAN_OPTIONS=verify_asan_link_order=0

python3 tools/odbc/test/pyodbc-test.py
if [[ $? != 0 ]]; then
    exit 1;
fi
