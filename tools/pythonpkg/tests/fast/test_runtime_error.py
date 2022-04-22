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
        with pytest.raises(Exception):
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
        with pytest.raises(Exception):
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
        with pytest.raises(Exception):
            res.fetch_arrow_reader(1)

    def test_relation_fetchall_error(self, duckdb_cursor):
        conn = duckdb.connect()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        conn.execute("create view x as select * from df_in")
        rel = conn.query("select * from x")
        del df_in
        with pytest.raises(Exception):
            rel.fetchall()

    def test_relation_fetchall_execute(self, duckdb_cursor):
        conn = duckdb.connect()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        conn.execute("create view x as select * from df_in")
        rel = conn.query("select * from x")
        del df_in
        with pytest.raises(Exception):
            rel.execute()

    def test_relation_query_error(self, duckdb_cursor):
        conn = duckdb.connect()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        conn.execute("create view x as select * from df_in")
        rel = conn.query("select * from x")
        del df_in
        with pytest.raises(Exception):
            rel.query("bla", "select * from bla")

    def test_conn_broken_statement_error(self, duckdb_cursor):
        conn = duckdb.connect()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        conn.execute("create view x as select * from df_in")
        del df_in
        with pytest.raises(Exception):
            conn.execute("select 1; select * from x; select 3;")

    def test_conn_prepared_statement_error(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table integers (a integer, b integer)")
        with pytest.raises(Exception):
            conn.execute("select * from integers where a =? and b=?",[1])

    def test_closed_conn_exceptions(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.close()
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})

        with pytest.raises(Exception):
            conn.register("bla",df_in)

        with pytest.raises(Exception):
            conn.from_query("select 1")

        with pytest.raises(Exception):
            conn.table("bla")

        with pytest.raises(Exception):
            conn.table("bla")

        with pytest.raises(Exception):
            conn.view("bla")

        with pytest.raises(Exception):
            conn.values("bla")

        with pytest.raises(Exception):
            conn.table_function("bla")

        with pytest.raises(Exception):
            conn.from_df("bla")

        with pytest.raises(Exception):
            conn.from_csv_auto("bla")

        with pytest.raises(Exception):
            conn.from_parquet("bla")

        with pytest.raises(Exception):
            conn.from_arrow("bla")

    def test_missing_result_from_conn_exceptions(self, duckdb_cursor):
        conn = duckdb.connect()

        with pytest.raises(Exception):
            conn.fetchone()

        with pytest.raises(Exception):
            conn.fetchall()

        with pytest.raises(Exception):
            conn.fetchnumpy()

        with pytest.raises(Exception):
            conn.fetchdf()

        with pytest.raises(Exception):
            conn.fetch_df_chunk()

        with pytest.raises(Exception):
            conn.fetch_arrow_table()

        with pytest.raises(Exception):
            conn.fetch_arrow_chunk()

        with pytest.raises(Exception):
            conn.fetch_record_batch()
