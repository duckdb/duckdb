#!/usr/bin/env bash
#Note: DONT run as root

set -e

mkdir -p data/parquet-testing/presigned

generate_large_parquet_query=$(cat <<EOF

CALL DBGEN(sf=1);
COPY lineitem TO 'data/parquet-testing/presigned/presigned-url-lineitem.parquet' (FORMAT 'parquet');

EOF
)
duckdb -c "$generate_large_parquet_query"

mkdir -p data/attach_test/

# Generate Storage Version
duckdb  data/attach_test/attach.db < test/sql/storage_version/generate_storage_version.sql
duckdb  data/attach_test/lineitem_sf1.db -c "CALL dbgen(sf=1)"
