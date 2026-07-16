#!/usr/bin/env python3
"""Generate TPC-H SF10 and store it across 1000 parquet files in ./partitioned_tpch."""

import duckdb

SF = 10
NUM_FILES = 1000
OUTPUT_DIR = "partitioned_tpch"

con = duckdb.connect()

# Generate TPC-H data
con.execute("INSTALL tpch; LOAD tpch;")
con.execute(f"CALL dbgen(sf={SF})")

# Spread lineitem (the largest table) across NUM_FILES parquet files using a
# partition column, then partition-write to the output directory.
con.execute(
    f"""
    COPY (
        SELECT *, (row_number() OVER () - 1) % {NUM_FILES} AS part
        FROM lineitem
    )
    TO '{OUTPUT_DIR}'
    (FORMAT PARQUET, PARTITION_BY (part), OVERWRITE_OR_IGNORE)
"""
)

print(f"Wrote TPC-H SF{SF} lineitem across {NUM_FILES} parquet files in ./{OUTPUT_DIR}")
