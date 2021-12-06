import duckdb
import pandas as pd

class TestConnectionTransaction(object):
    def test_transaction(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute('create table t (i integer)')
        con.execute ('insert into t values (1)')

        con.begin()
        con.execute ('insert into t values (1)')
        assert con.execute('select count (*) from t').fetchone()[0] == 2
        con.rollback()
        assert con.execute('select count (*) from t').fetchone()[0] == 1
        con.begin()
        con.execute ('insert into t values (1)')
        assert con.execute('select count (*) from t').fetchone()[0] == 2
        con.commit()
        assert con.execute('select count (*) from t').fetchone()[0] == 2
