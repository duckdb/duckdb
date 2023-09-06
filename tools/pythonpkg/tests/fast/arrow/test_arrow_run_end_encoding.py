import duckdb
import pytest
import pandas as pd
import duckdb

pa = pytest.importorskip("pyarrow", '14.0.0.dev')
import pyarrow.compute as pc


class TestArrowREE(object):
    def test_arrow_run_end_encoding(self):
        con = duckdb.connect()

        rel = con.sql(
            """
            select (i // 4) as ree from range(10000) t(i);
        """
        )
        array = rel.arrow()['ree']
        expected = rel.fetchall()

        encoded_array = pc.run_end_encode(array)

        schema = pa.schema([("ree", encoded_array.type)])
        tbl = pa.Table.from_arrays([encoded_array], schema=schema)
        res = duckdb.sql("select * from tbl").fetchall()
        assert res == expected

    def test_run_bigger_than_vector(self):
        con = duckdb.connect()

        rel = con.sql("""
            select (i // 2392) as ree from range(100000) t(i);
        """)
        array = rel.arrow()['ree']
        expected = rel.fetchall()

        encoded_array = pc.run_end_encode(array)

        schema = pa.schema([("ree", encoded_array.type)])
        tbl = pa.Table.from_arrays([encoded_array], schema=schema)
        res = duckdb.sql("select * from tbl").fetchall()
        assert res == expected
