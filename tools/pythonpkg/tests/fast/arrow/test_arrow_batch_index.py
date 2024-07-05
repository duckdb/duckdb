import duckdb
import pytest
import pandas as pd
import duckdb

pa = pytest.importorskip("pyarrow")


class TestArrowBatchIndex(object):
    def test_arrow_batch_index(self, duckdb_cursor):
        con = duckdb.connect()
        df = con.execute('SELECT * FROM range(10000000) t(i)').df()
        arrow_tbl = pa.Table.from_pandas(df)

        con.execute('CREATE TABLE tbl AS SELECT * FROM arrow_tbl')

        result = con.execute('SELECT * FROM tbl LIMIT 5').fetchall()
        assert [x[0] for x in result] == [0, 1, 2, 3, 4]

        result = con.execute('SELECT * FROM tbl LIMIT 5 OFFSET 777778').fetchall()
        assert [x[0] for x in result] == [777778, 777779, 777780, 777781, 777782]
