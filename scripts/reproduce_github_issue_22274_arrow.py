#!/usr/bin/env python3
"""
Manual reproducer for https://github.com/duckdb/duckdb/issues/22274

Uses pyarrow + DuckDB register() with produce_arrow_string_view and Arrow 1.5 output,
matching the failure mode discussed on the issue and in test/arrow/arrow_filter_pushdown.cpp.

Requires: pip install duckdb pyarrow

Run (from repo root, with a duckdb build on PYTHONPATH or default pip duckdb):
  python3 scripts/reproduce_github_issue_22274_arrow.py
"""

from __future__ import annotations

import sys

try:
	import duckdb
except ImportError as e:
	print("Install duckdb: pip install duckdb", file=sys.stderr)
	raise SystemExit(1) from e

try:
	import pyarrow  # noqa: F401 — required for Relation.arrow() / register()
except ImportError as e:
	print("Install pyarrow: pip install pyarrow", file=sys.stderr)
	raise SystemExit(1) from e


def main() -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute("SET produce_arrow_string_view = true")
    con.execute("SET arrow_output_version = '1.5'")

    con.execute(
        """
		CREATE TABLE src AS SELECT
			['TOYOTA','HONDA','FORD','CHEVROLET','BMW','MERCEDES','NISSAN','HYUNDAI','KIA','SUBARU',
			 'MAZDA','VPG','ISUZU','SMART','LUCID','LOTUS','PLYMOUTH','DODGE_MITS','VINFAST',
			 'POLESTAR'][((i % 20) + 1)::INTEGER] AS name,
			CASE WHEN i % 10 != 0 THEN 0::TINYINT ELSE 1::TINYINT END AS flag
		FROM range(50000) tbl(i)
	"""
    )

    rel = con.sql("SELECT name, flag FROM src")
    arrow_tbl = rel.arrow()
    # register as in-memory Arrow table (same integration surface as many pyarrow workflows)
    con.register("data", arrow_tbl)

    sql = """
	SELECT name, COUNT(*) AS cnt FROM data
	WHERE flag < 1 AND name IN ('VPG','ISUZU','SMART','LUCID','LOTUS','POLESTAR')
	GROUP BY 1 ORDER BY 1
	"""
    try:
        print(con.execute(sql).fetchall())
        print("OK: query completed without error.")
    except duckdb.Error as e:
        print(f"Query failed: {e}", file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
