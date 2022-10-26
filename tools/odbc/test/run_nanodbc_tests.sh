#!/bin/bash

set -e

#echo -e "[ODBC]\nTrace = yes\nTraceFile = /tmp/odbctrace\n\n[DuckDB Driver]\nDriver = "$(pwd)"/build/debug/tools/odbc/libduckdb_odbc.so" > ~/.odbcinst.ini
#echo -e "[DuckDB]\nDriver = DuckDB Driver\nDatabase=:memory:\n" > ~/.odbc.ini

BASE_DIR=$(dirname $0)

#Configuring ODBC files
$BASE_DIR/../linux_setup/unixodbc_setup.sh -u -D $(pwd)/build/debug/tools/odbc/libduckdb_odbc.so


export NANODBC_TEST_CONNSTR_ODBC="DRIVER=DuckDB Driver;"
export ASAN_OPTIONS=verify_asan_link_order=0

declare -a SUPPORTED_TESTS
SUPPORTED_TESTS[1]=test_simple
SUPPORTED_TESTS[2]=test_driver
SUPPORTED_TESTS[3]=test_null
SUPPORTED_TESTS[4]=test_connection_environment
SUPPORTED_TESTS[5]=test_get_info
SUPPORTED_TESTS[6]=test_decimal_conversion
SUPPORTED_TESTS[7]=test_execute_multiple_transaction
SUPPORTED_TESTS[8]=test_execute_multiple
SUPPORTED_TESTS[9]=test_move
SUPPORTED_TESTS[10]=test_nullptr_nulls
SUPPORTED_TESTS[11]=test_result_iterator
SUPPORTED_TESTS[12]=test_transaction
SUPPORTED_TESTS[13]=test_while_not_end_iteration
SUPPORTED_TESTS[14]=test_while_next_iteration
SUPPORTED_TESTS[15]=test_catalog_list_schemas
SUPPORTED_TESTS[16]=test_catalog_list_catalogs
SUPPORTED_TESTS[17]=test_blob

for test in ${SUPPORTED_TESTS[@]}
do
    echo "$test"
    ./nanodbc/build/test/odbc_tests $test
    if [[ $? != 0 ]]; then
        exit 1
    fi
done
