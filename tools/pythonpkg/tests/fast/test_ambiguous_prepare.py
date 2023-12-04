import duckdb
import pandas as pd
import pytest


class TestAmbiguousPrepare(object):
    def test_bool(self, duckdb_cursor):
        conn = duckdb.connect()
        res = conn.execute("select ?, ?, ?", (True, 42, [1, 2, 3])).fetchall()
        assert res[0][0] == True
        assert res[0][1] == 42
        assert res[0][2] == [1, 2, 3]
