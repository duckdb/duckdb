import duckdb
import pandas as pd
import pytest

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

    def test_register_error(self, duckdb_cursor):
        con = duckdb.connect()
        py_obj = "this is a string"
        with pytest.raises(ValueError):
            con.register(py_obj, "v")

    def test_arrow_fetch_table_error(self, duckdb_cursor):
        try:
            import pyarrow as pa
        except:
            return
        con = duckdb.connect()
        arrow_object = con.execute("select 1").arrow()
        arrow_relation = con.from_arrow(arrow_object)
        res = arrow_relation.execute()
        res.close()
        with pytest.raises(ValueError):
            res.fetch_arrow_table()

    def test_arrow_record_batch_reader_error(self, duckdb_cursor):
        try:
            import pyarrow as pa
        except:
            return
        con = duckdb.connect()
        arrow_object = con.execute("select 1").arrow()
        arrow_relation = con.from_arrow(arrow_object)
        res = arrow_relation.execute()
        res.close()
        with pytest.raises(ValueError):
            res.fetch_arrow_reader(1)

    def test_relation_fetchall_error(self, duckdb_cursor):
        conn = duckdb.connect()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        conn.execute("create view x as select * from df_in")
        rel = conn.query("select * from x")
        del df_in
        with pytest.raises(duckdb.StandardException):
            rel.fetchall()

    def test_relation_fetchall_execute(self, duckdb_cursor):
        conn = duckdb.connect()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        conn.execute("create view x as select * from df_in")
        rel = conn.query("select * from x")
        del df_in
        with pytest.raises(duckdb.CatalogException):
            rel.execute()

    def test_relation_query_error(self, duckdb_cursor):
        conn = duckdb.connect()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        conn.execute("create view x as select * from df_in")
        rel = conn.query("select * from x")
        del df_in
        with pytest.raises(ValueError):
            rel.query("bla", "select * from bla")

    def test_conn_broken_statement_error(self, duckdb_cursor):
        conn = duckdb.connect()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        conn.execute("create view x as select * from df_in")
        del df_in
        with pytest.raises(duckdb.InvalidInputException):
            conn.execute("select 1; select * from x; select 3;")

    def test_conn_prepared_statement_error(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table integers (a integer, b integer)")
        with pytest.raises(duckdb.InvalidInputException):
            conn.execute("select * from integers where a =? and b=?",[1])

    def test_closed_conn_exceptions(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.close()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})

        with pytest.raises(ValueError):
            conn.register("bla",df_in)

        with pytest.raises(ValueError):
            conn.from_query("select 1")

        with pytest.raises(ValueError):
            conn.table("bla")

        with pytest.raises(ValueError):
            conn.table("bla")

        with pytest.raises(ValueError):
            conn.view("bla")

        with pytest.raises(ValueError):
            conn.values("bla")

        with pytest.raises(ValueError):
            conn.table_function("bla")

        with pytest.raises(ValueError):
            conn.from_df("bla")

        with pytest.raises(ValueError):
            conn.from_csv_auto("bla")

        with pytest.raises(ValueError):
            conn.from_parquet("bla")

        with pytest.raises(ValueError):
            conn.from_arrow("bla")

    def test_missing_result_from_conn_exceptions(self, duckdb_cursor):
        conn = duckdb.connect()

        with pytest.raises(ValueError):
            conn.fetchone()

        with pytest.raises(ValueError):
            conn.fetchall()

        with pytest.raises(ValueError):
            conn.fetchnumpy()

        with pytest.raises(ValueError):
            conn.fetchdf()

        with pytest.raises(ValueError):
            conn.fetch_df_chunk()

        with pytest.raises(ValueError):
            conn.fetch_arrow_table()

        with pytest.raises(ValueError):
            conn.fetch_arrow_chunk()

        with pytest.raises(ValueError):
            conn.fetch_record_batch()
