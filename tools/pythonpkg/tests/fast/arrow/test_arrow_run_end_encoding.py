import duckdb
import pytest
import pandas as pd
import duckdb

pa = pytest.importorskip("pyarrow", '14.0.0.dev')
import pyarrow.compute as pc


class TestArrowREE(object):
    def test_arrow_run_end_encoding(self):
        con = duckdb.connect()

        rel = con.sql("""
            select (i // 4) as ree from range(1000) t(i);
        """)
        array = rel.arrow()['ree']

        encoded_array = pc.run_end_encode(array)

        schema = pa.schema([("ree", encoded_array.type)])
        tbl = pa.Table.from_arrays([encoded_array], schema=schema)
        res = duckdb.sql("select * from tbl").fetchall()
        print(res)
