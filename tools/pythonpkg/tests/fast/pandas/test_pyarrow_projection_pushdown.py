import duckdb
import os
import pytest

from conftest import pandas_supports_arrow_backend

pa = pytest.importorskip("pyarrow")
ds = pytest.importorskip("pyarrow.dataset")
_ = pytest.importorskip("pandas", '2.0.0')


@pytest.mark.skipif(not pandas_supports_arrow_backend(), reason="pandas does not support the 'pyarrow' backend")
class TestArrowDFProjectionPushdown(object):
    def test_projection_pushdown_no_filter(self, duckdb_cursor):
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("CREATE TABLE test (a  INTEGER, b INTEGER, c INTEGER)")
        duckdb_conn.execute("INSERT INTO  test VALUES (1,1,1),(10,10,10),(100,10,100),(NULL,NULL,NULL)")
        duck_tbl = duckdb_conn.table("test")
        arrow_table = duck_tbl.df().convert_dtypes(dtype_backend='pyarrow')
        duckdb_conn.register("testarrowtable", arrow_table)
        assert duckdb_conn.execute("SELECT sum(a) FROM  testarrowtable").fetchall() == [(111,)]
