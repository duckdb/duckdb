import pandas as pd
import numpy as np
import duckdb
import os
import tempfile


class TestNonDefaultConn(object):
    def test_values(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        duckdb.values([1], conn).insert_into("t")
        assert conn.execute("select count(*) from t").fetchall()[0] == (1,)

    def test_query(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1)")
        assert duckdb.query("select count(*) from t", connection=conn).execute().fetchall()[0] == (1,)
        assert duckdb.from_query("select count(*) from t", connection=conn).execute().fetchall()[0] == (1,)

    def test_from_csv(self, duckdb_cursor):
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1)")
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4]})
        test_df.to_csv(temp_file_name, index=False)
        rel = duckdb.from_csv_auto(temp_file_name, conn)
        assert rel.query('t_2', 'select count(*) from t inner join t_2 on (a = i)').fetchall()[0] == (1,)

    def test_from_parquet(self, duckdb_cursor):
        try:
            import pyarrow as pa
        except:
            return
        temp_file_name = os.path.join(tempfile.mkdtemp(), next(tempfile._get_candidate_names()))
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1)")
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4]})
        test_df.to_parquet(temp_file_name, index=False)
        rel = duckdb.from_parquet(temp_file_name, connection=conn)
        assert rel.query('t_2', 'select count(*) from t inner join t_2 on (a = i)').fetchall()[0] == (1,)

    def test_from_df(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1)")
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4]})
        rel = duckdb.df(test_df, connection=conn)
        assert rel.query('t_2', 'select count(*) from t inner join t_2 on (a = i)').fetchall()[0] == (1,)
        rel = duckdb.from_df(test_df, connection=conn)
        assert rel.query('t_2', 'select count(*) from t inner join t_2 on (a = i)').fetchall()[0] == (1,)

    def test_from_arrow(self, duckdb_cursor):
        try:
            import pyarrow as pa
        except:
            return

        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1)")
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4]})
        test_arrow = pa.Table.from_pandas(test_df)
        rel = duckdb.from_arrow(test_arrow, connection=conn)
        assert rel.query('t_2', 'select count(*) from t inner join t_2 on (a = i)').fetchall()[0] == (1,)
        rel = duckdb.arrow(test_arrow, connection=conn)
        assert rel.query('t_2', 'select count(*) from t inner join t_2 on (a = i)').fetchall()[0] == (1,)

    def test_filter_df(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1), (4)")
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4]})
        rel = duckdb.filter(test_df, "i < 2", connection=conn)
        assert rel.query('t_2', 'select count(*) from t inner join t_2 on (a = i)').fetchall()[0] == (1,)

    def test_project_df(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1), (4)")
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": [1, 2, 3, 4]})
        rel = duckdb.project(test_df, "i", connection=conn)
        assert rel.query('t_2', 'select * from t inner join t_2 on (a = i)').fetchall()[0] == (1, 1)

    def test_agg_df(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1), (4)")
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": [1, 2, 3, 4]})
        rel = duckdb.aggregate(test_df, "count(*) as i", connection=conn)
        assert rel.query('t_2', 'select * from t inner join t_2 on (a = i)').fetchall()[0] == (4, 4)

    def test_distinct_df(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1)")
        test_df = pd.DataFrame.from_dict({"i": [1, 1, 2, 3, 4]})
        rel = duckdb.distinct(test_df, connection=conn)
        assert rel.query('t_2', 'select * from t inner join t_2 on (a = i)').fetchall()[0] == (1, 1)

    def test_limit_df(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1),(4)")
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4]})
        rel = duckdb.limit(test_df, 1, connection=conn)
        assert rel.query('t_2', 'select * from t inner join t_2 on (a = i)').fetchall()[0] == (1, 1)

    def test_query_df(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1),(4)")
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4]})
        rel = duckdb.query_df(test_df, 't_2', 'select * from t inner join t_2 on (a = i)', connection=conn)
        assert rel.fetchall()[0] == (1, 1)

    def test_query_order(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table t (a integer)")
        conn.execute("insert into t values (1),(4)")
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4]})
        rel = duckdb.order(test_df, 'i', connection=conn)
        assert rel.query('t_2', 'select * from t inner join t_2 on (a = i)').fetchall()[0] == (1, 1)
