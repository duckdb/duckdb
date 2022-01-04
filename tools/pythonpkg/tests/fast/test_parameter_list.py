import duckdb
import pandas as pd
import pytest

class TestParameterList(object): 
    def test_bool(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table bool_table (a bool)")
        conn.execute("insert into bool_table values (TRUE)")
        res = conn.execute("select count(*) from bool_table where a =?",[True])
        assert res.fetchone()[0] == 1

    def test_exception(self, duckdb_cursor):
        conn = duckdb.connect()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        conn.execute("create table bool_table (a bool)")
        conn.execute("insert into bool_table values (TRUE)")
        with pytest.raises(Exception):
            res = conn.execute("select count(*) from bool_table where a =?",[df_in])
