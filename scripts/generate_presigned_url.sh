#!/usr/bin/env bash
#Note: DONT run as root

mkdir -p data/parquet-testing/presigned

generate_large_parquet_query=$(cat <<EOF

CALL DBGEN(sf=1);
COPY lineitem TO 'data/parquet-testing/presigned/presigned-url-lineitem.parquet' (FORMAT 'parquet');

EOF
)
build/release/duckdb -c "$generate_large_parquet_query"