import duckdb
import os
import pytest


class TestArrowProjectionPushdown(object):
    def test_projection_pushdown_no_filter(self, duckdb_cursor):
        pa = pytest.importorskip("pyarrow")
        ds = pytest.importorskip("pyarrow.dataset")

        duckdb_cursor.execute(
            """
            CREATE TABLE test (a  INTEGER, b INTEGER, c INTEGER)
        """
        )
        duckdb_cursor.execute(
            """
            INSERT INTO test VALUES
                (1,2,3),
                (10,20,30),
                (100,200,300),
                (NULL,NULL,NULL)
        """
        )
        duck_tbl = duckdb_cursor.table("test")
        arrow_table = duck_tbl.arrow()
        assert duckdb_cursor.execute("SELECT sum(c) FROM arrow_table").fetchall() == [(333,)]

        # RecordBatch does not use projection pushdown, test that this also still works
        record_batch = arrow_table.to_batches()[0]
        assert duckdb_cursor.execute("SELECT sum(c) FROM record_batch").fetchall() == [(333,)]

        arrow_dataset = ds.dataset(arrow_table)
        assert duckdb_cursor.execute("SELECT sum(c) FROM arrow_dataset").fetchall() == [(333,)]
