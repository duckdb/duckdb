#!/bin/sh

rm -rf .eggs .pytest_cache build dist duckdb.egg-info duckdb.cpp duckdb.hpp parquet-extension.cpp parquet-extension.hpp duckdb duckdb_tarball
rm -f sources.list includes.list githash.list

while [[ $# -ge 1 ]]
do
key="$1"
  case $key in
    -f | --force)
    python3 clean.py -f
        exit 1
        ;;
  esac
shift
done
python3 clean.py