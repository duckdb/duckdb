import duckdb
import numpy as np
import datetime
import pytest


class TestNumpyDatetime64(object):
    def test_numpy_datetime64(self, duckdb_cursor):
        duckdb_con = duckdb.connect()

        duckdb_con.execute("create table tbl(col TIMESTAMP)")
        duckdb_con.execute(
            "insert into tbl VALUES (CAST(? AS TIMESTAMP WITHOUT TIME ZONE))",
            parameters=[np.datetime64('2022-02-08T06:01:38.761310')],
        )
        assert [(datetime.datetime(2022, 2, 8, 6, 1, 38, 761310),)] == duckdb_con.execute(
            "select * from tbl"
        ).fetchall()

    def test_numpy_datetime_overflow(self):
        duckdb_con = duckdb.connect()

        duckdb_con.execute("create table test (date DATE)")
        duckdb_con.execute("INSERT INTO TEST VALUES ('2263-02-28')")

        with pytest.raises(duckdb.ConversionException):
            res1 = duckdb_con.execute("select * from test").fetchnumpy()
