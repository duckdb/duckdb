import duckdb

class TestRuntimeError(object):
    def test_fetch_error(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute("create table tbl as select 'hello' i")
        raised_error = False
        try:
            con.execute("select i::int from tbl").fetchall()
        except:
            raised_error = True
        assert raised_error == True

    def test_df_error(self, duckdb_cursor):
        con = duckdb.connect()
        con.execute("create table tbl as select 'hello' i")
        raised_error = False
        try:
            con.execute("select i::int from tbl").df()
        except:
            raised_error = True
        assert raised_error == True

    def test_arrow_error(self, duckdb_cursor):
        try:
            import pyarrow
        except:
            return
        con = duckdb.connect()
        con.execute("create table tbl as select 'hello' i")
        raised_error = False
        try:
            con.execute("select i::int from tbl").arrow()
        except:
            raised_error = True
        assert raised_error == True
