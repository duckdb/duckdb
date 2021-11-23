import duckdb
import pandas as pd
import numpy
import datetime
import time

class TestProgressBarPandas(object):

    def test_progress_pandas_single(self, duckdb_cursor):
        con = duckdb.connect()
        df = pd.DataFrame({'i': numpy.arange(10000000)})

        con.register('df', df)
        con.register('df_2', df)
        con.execute("PRAGMA progress_bar_time=1")
        con.execute("PRAGMA disable_print_progress_bar")
        result =  con.execute("SELECT SUM(df.i) FROM df inner join df_2 on (df.i = df_2.i)").fetchall()
        assert result[0][0] == 49999995000000


    def test_progress_pandas_parallel(self,duckdb_cursor):
        con = duckdb.connect()
        df = pd.DataFrame({'i': numpy.arange(10000000)})

        con.register('df', df)
        con.register('df_2', df)
        con.execute("PRAGMA progress_bar_time=1")
        con.execute("PRAGMA disable_print_progress_bar")
        con.execute("PRAGMA threads=4")
        parallel_results = con.execute("SELECT SUM(df.i) FROM df inner join df_2 on (df.i = df_2.i)").fetchall()
        assert parallel_results[0][0] == 49999995000000

    def test_progress_pandas_empty(self,duckdb_cursor):
        con = duckdb.connect()
        df = pd.DataFrame({'i' : []})
        con.register('df', df)
        con.execute("PRAGMA progress_bar_time=1")
        con.execute("PRAGMA disable_print_progress_bar")
        result =  con.execute("SELECT SUM(df.i) from df").fetchall()
        assert result[0][0] == None