import duckdb
import numpy as np
import datetime

class TestNumpyDatetime64(object):
    def test_numpy_datetime64(self, duckdb_cursor):
        duckdb_con = duckdb.connect()

        duckdb_con.execute("create table tbl(col TIMESTAMP)")
        duckdb_con.execute("insert into tbl VALUES (CAST(? AS TIMESTAMP WITHOUT TIME ZONE))", parameters=[np.datetime64('2022-02-08T06:01:38.761310')])
        assert [(datetime.datetime(2022, 2, 8, 6, 1, 38, 761310),)] == duckdb_con.execute("select * from tbl").fetchall()


