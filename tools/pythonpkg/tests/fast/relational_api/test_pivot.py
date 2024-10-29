import duckdb
import pytest
import os
import tempfile


class TestPivot(object):
    def test_pivot_issue_14600(self, duckdb_cursor):
        duckdb_cursor.sql(
            "create table input_data as select unnest(['u','v','w']) as a, unnest(['x','y','z']) as b, unnest([1,2,3]) as c;"
        )
        pivot_1 = duckdb_cursor.query("pivot input_data on a using max(c) group by b;")
        pivot_2 = duckdb_cursor.query("pivot input_data on b using max(c) group by a;")
        pivot_1.create("pivot_1")
        pivot_2.create("pivot_2")
        pivot_1_tbl = duckdb_cursor.table("pivot_1")
        pivot_2_tbl = duckdb_cursor.table("pivot_2")
        assert set(pivot_1.columns) == set(pivot_1_tbl.columns)
        assert set(pivot_2.columns) == set(pivot_2_tbl.columns)

    def test_pivot_issue_14601(self, duckdb_cursor):
        duckdb_cursor.sql(
            "create table input_data as select unnest(['u','v','w']) as a, unnest(['x','y','z']) as b, unnest([1,2,3]) as c;"
        )
        pivot_1 = duckdb_cursor.query("pivot input_data on a using max(c) group by b;")
        pivot_1.create("pivot_1")
        export_dir = tempfile.mkdtemp()
        duckdb_cursor.query(f"EXPORT DATABASE '{export_dir}'")
        with open(os.path.join(export_dir, "schema.sql"), "r") as f:
            assert 'CREATE TYPE' not in f.read()
