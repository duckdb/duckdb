import duckdb
import pandas as pd
import numpy
import datetime
import time


class TestPartitionedPandasScan(object):
    def test_parallel_pandas(self, duckdb_cursor):
        con = duckdb.connect()
        df = pd.DataFrame({'i': numpy.arange(10000000)})

        con.register('df', df)

        seq_results = con.execute("SELECT SUM(i) FROM df").fetchall()

        con.execute("PRAGMA threads=4")
        parallel_results = con.execute("SELECT SUM(i) FROM df").fetchall()

        assert seq_results[0][0] == 49999995000000
        assert parallel_results[0][0] == 49999995000000
