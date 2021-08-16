#!/bin/bash

echo -e "[ODBC]\nTrace = yes\nTraceFile = /tmp/odbctrace\n\n[DuckDB Driver]\nDriver = "$(pwd)"/build/debug/tools/odbc/libduckdb_odbc.so" > ~/.odbcinst.ini
echo -e "[DuckDB]\nDriver = DuckDB Driver\nDatabase=test.db\n" > ~/.odbc.ini

export ASAN_OPTIONS=verify_asan_link_order=0

python tools/odbc/test/isql-test.py isql
if [[ $? != 0 ]]; then
    exit 1;
fi
    
# running isql with the option -e
rm test.db && python tools/odbc/test/isql-test.py isql -e
if [[ $? != 0 ]]; then
    exit 1;
fi
