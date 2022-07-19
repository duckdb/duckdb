#!/bin/bash

set -e

#echo -e "[ODBC]\nTrace = yes\nTraceFile = /tmp/odbctrace\n\n[DuckDB Driver]\nDriver = "$(pwd)"/build/debug/tools/odbc/libduckdb_odbc.so" > ~/.odbcinst.ini
#echo -e "[DuckDB]\nDriver = DuckDB Driver\nDatabase=:memory:\n" > ~/.odbc.ini

BASE_DIR=$(dirname $0)

#Configuring ODBC files
$BASE_DIR/../linux_setup/unixodbc_setup.sh -u -db :memory: -D $(pwd)/build/debug/tools/odbc/libduckdb_odbc.so

export PSQLODBC_TEST_DSN="DuckDB"

TRACE_FILE=/tmp/odbctrace
BASE_DUCKDB_DIR=$(pwd)
cd psqlodbc
export PSQLODBC_DIR=$(pwd)

# creating contrib_regression database used by some tests
rm -f contrib_regression
rm -f contrib_regression.wal
./build/debug/reset-db < sampletables.sql
if [[ $? != 0 ]]; then
    cat $TRACE_FILE
    exit 1
fi

# running supported tests
while read test_file
do
    ./build/debug/psql_odbc_test $test_file
    if [[ $? != 0 ]]; then
        cat $TRACE_FILE
        exit 1
    fi
    # clean odbc trace file
    rm $TRACE_FILE
done < ${BASE_DUCKDB_DIR}/tools/odbc/test/psql_supported_tests

