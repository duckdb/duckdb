#!/bin/bash

function Usage() {
    echo "Usage: $0 [-s | -u]"
    echo "-s:  using 'sudo' to add DuckDB connection to the system-level: /etc/odbc[inst].ini"
    echo "-u:  add DuckDB connection to the user-level ~/odbc[inst].ini file."
    exit 1
}

function CreateODBCIniFile() {
cat << EOF > $1
[DuckDB]
Driver = DuckDB Driver
Database=:memory:
EOF
}

function CreateODBCInstFile() {
cat << EOF > $1
[ODBC]
Trace = yes
TraceFile = /tmp/odbctrace

[DuckDB Driver]
Driver = $BASE_DIR/libduckdb_odbc.so
EOF
}

if (($# != 1)); then
    Usage $0
fi

BASE_DIR=$(pwd)
ODBC_INI_FILE=$(mktemp)
ODBCINST_FILE=$(mktemp)

CreateODBCIniFile $ODBC_INI_FILE
CreateODBCInstFile $ODBCINST_FILE

case $1 in
    "-s")
        # checking for root level
        if (( $EUID != 0 )); then
            echo "Please run as root"
            Usage $0        
        fi
        odbcinst -i -d -f $ODBCINST_FILE
        odbcinst -i -s -l -f $ODBC_INI_FILE
        ;;
    "-u")
        cat $ODBCINST_FILE >> ~/.odbcinst.ini
        odbcinst -i -s -h -f $ODBC_INI_FILE
        ;;
    *)
        Usage $0
        ;;
esac

rm $ODBC_INI_FILE
rm $ODBCINST_FILE
