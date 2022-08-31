import duckdb
from pandas import DataFrame
import pytest

class TestInsertInto(object):
    def test_insert_into_schema(self, duckdb_cursor):
        # open connection
        con = duckdb.connect()
        con.execute('CREATE SCHEMA s')
        con.execute('CREATE TABLE s.t (id INTEGER PRIMARY KEY)')

        # make relation
        df = DataFrame([1],columns=['id'])
        rel = con.from_df(df)

        rel.insert_into('s.t')

        assert con.execute("select * from s.t").fetchall() == [(1,)]

        # This should fail since this will go to default schema
        with pytest.raises(duckdb.Error):
            rel.insert_into('t')

        #If we add t in the default schema it should work.
        con.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')
        rel.insert_into('t')
        assert con.execute("select * from t").fetchall() == [(1,)]
