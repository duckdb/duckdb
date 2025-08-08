#!/bin/bash
rm -rf gen-cpp
thrift --gen "cpp:moveable_types,no_default_operators=true" parquet.thrift
cp gen-cpp/* .
sed -i .bak -e "s/std::vector/duckdb::vector/" parquet_types.*
sed -i .bak -e 's/namespace duckdb_parquet {/#include "windows_compatibility.h"\nnamespace apache = duckdb_apache;\n\nnamespace duckdb_parquet {/' parquet_types.h
rm *.bak
rm -rf gen-cpp