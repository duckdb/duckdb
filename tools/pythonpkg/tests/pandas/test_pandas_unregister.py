import duckdb
import pandas as pd
import pytest
import tempfile
import os
import gc

class TestPandasUnregister(object):
    def test_pandas_unregister1(self, duckdb_cursor):
        df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        connection = duckdb.connect(":memory:")
        connection.register("dataframe", df)

        df2 = connection.execute("SELECT * FROM dataframe;").fetchdf()
        connection.unregister("dataframe")
        with pytest.raises(RuntimeError):
            connection.execute("SELECT * FROM dataframe;").fetchdf()
        with pytest.raises(RuntimeError):
            connection.execute("DROP VIEW dataframe;")
        connection.execute("DROP VIEW IF EXISTS dataframe;")


    def test_pandas_unregister2(self, duckdb_cursor):
        fd, db = tempfile.mkstemp()
        os.close(fd)
        os.remove(db)

        connection = duckdb.connect(db)
        df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])

        connection.register("dataframe", df)
        connection.unregister("dataframe")  # Attempting to unregister.
        connection.close()

        # Reconnecting while DataFrame still in mem.
        connection = duckdb.connect(db)
        assert len(connection.execute("PRAGMA show_tables;").fetchall()) == 0

        with pytest.raises(RuntimeError):
            connection.execute("SELECT * FROM dataframe;").fetchdf()

        connection.close()

        del df
        gc.collect()

        # Reconnecting after DataFrame freed.
        connection = duckdb.connect(db)
        assert len(connection.execute("PRAGMA show_tables;").fetchall()) == 0
        with pytest.raises(RuntimeError):
            connection.execute("SELECT * FROM dataframe;").fetchdf()
        connection.close()
