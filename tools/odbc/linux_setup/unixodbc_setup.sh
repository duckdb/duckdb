#!/bin/bash

function Usage() {
    printf "Usage: $0 <level> [options]\n\n"
    printf "Example: $0 -u -db ~/database_path -D ~/driver_path/libduckdb_odbc.so\n\n"
    echo "Level:"
    echo "-s: System-level, using 'sudo' to configure DuckDB ODBC at the system-level, changing the files: /etc/odbc[inst].ini"
    echo "-u: User-level, configuring the DuckDB ODBC at the user-level, changing the files: ~/.odbc[inst].ini."
    printf "\nOptions:\n"
    echo "-db database_path>: the DuckDB database file path, the default is ':memory:' if not provided."
    echo "-D driver_path: the driver file path (i.e., the path for libduckdb_odbc.so), the default is using the base script directory"
    echo ""
    exit 1
}

function ReadArgs() {
    shift
    while (( $# > 1 ))
    do
        case $1 in
            "-db")
                shift
                DATABASE_PATH=$1
                shift
                ;;
            "-D")
                shift
                DRIVER_PATH=$1
                if grep -qv "libduckdb_odbc.so" <<< $DRIVER_PATH; then
                    printf "\n****Driver path doesn't contain 'libduckdb_odbc.so'****\n\n"
                    Usage
                fi
                shift
                ;;
            *)
                Usage
                ;;
        esac
    done
}


function CreateODBCIniFile() {
cat << EOF > $1
[DuckDB]
Driver = DuckDB Driver
Database=${DATABASE_PATH}
EOF
}

function CreateODBCInstFile() {
cat << EOF > $1
[ODBC]
Trace = yes
TraceFile = /tmp/odbctrace

[DuckDB Driver]
Driver = ${DRIVER_PATH}
EOF
}

function ConfigUserInstFile() {
    INST_SETUP=$1
    if test -f ~/.odbcinst.ini; then
        #file already exist
        sed -i "/DuckDB Driver/{n;s#.*libduckdb_odbc.so#Driver=${DRIVER_PATH}#}" ~/.odbcinst.ini
    else
        cp $INST_SETUP ~/.odbcinst.ini
    fi
}

# Exit immediately if a command exits with a non-zero status.
set -e

if (($# != 1 && $# != 3 && $# != 5)); then
    Usage
fi

# global vars
BASE_DIR=$(pwd)
DRIVER_PATH=$BASE_DIR/libduckdb_odbc.so
DATABASE_PATH=":memory:"

# Get the Database and Driver path from program arguments
ReadArgs $@

ODBC_INI_FILE=$(mktemp)
ODBCINST_FILE=$(mktemp)

CreateODBCIniFile $ODBC_INI_FILE
CreateODBCInstFile $ODBCINST_FILE

case $1 in
    "-s")
        # checking for root level
        if (( $EUID != 0 )); then
            printf "****Please run as root****\n\n"
            Usage
        fi
        odbcinst -i -d -f $ODBCINST_FILE
        odbcinst -i -s -l -f $ODBC_INI_FILE
        ;;
    "-u")
        ConfigUserInstFile $ODBCINST_FILE
        odbcinst -i -s -h -f $ODBC_INI_FILE
        ;;
    *)
        Usage
        ;;
esac

rm $ODBC_INI_FILE
rm $ODBCINST_FILE
