import duckdb
import os
import pytest

try:
    import pyarrow as pa
    import pyarrow.dataset as ds

    can_run = True
except:
    can_run = False


class TestArrowProjectionPushdown(object):
    def test_projection_pushdown_no_filter(self, duckdb_cursor):
        if not can_run:
            return
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("CREATE TABLE test (a  INTEGER, b INTEGER, c INTEGER)")
        duckdb_conn.execute("INSERT INTO  test VALUES (1,1,1),(10,10,10),(100,10,100),(NULL,NULL,NULL)")
        duck_tbl = duckdb_conn.table("test")
        arrow_table = duck_tbl.arrow()
        duckdb_conn.register("testarrowtable", arrow_table)
        assert duckdb_conn.execute("SELECT sum(a) FROM  testarrowtable").fetchall() == [(111,)]

        arrow_dataset = ds.dataset(arrow_table)
        duckdb_conn.register("testarrowdataset", arrow_dataset)
        assert duckdb_conn.execute("SELECT sum(a) FROM  testarrowdataset").fetchall() == [(111,)]
