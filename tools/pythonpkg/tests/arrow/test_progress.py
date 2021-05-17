import duckdb
import os
import sys
try:
    import pyarrow
    import pyarrow.parquet
    import numpy as np
    can_run = True
except:
    can_run = False

class TestProgressBarArrow(object):

    def test_progress_arrow(self, duckdb_cursor):
        if not can_run:
            return

        data = (pyarrow.array(np.arange(10000000), type=pyarrow.int32()))
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA set_progress_bar_time=1")
        duckdb_conn.execute("PRAGMA disable_print_progress_bar")

        tbl = pyarrow.Table.from_arrays([data],['a'])
        rel = duckdb_conn.from_arrow_table(tbl)
        result = rel.aggregate('sum(a)')
        assert (result.execute().fetchone()[0] == 49999995000000)
        # Multiple Threads
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA force_parallelism")
        assert (result.execute().fetchone()[0] == 49999995000000)

        # More than one batch
        tbl = pyarrow.Table.from_batches(pyarrow.Table.from_arrays([data],['a']).to_batches(100))
        rel = duckdb_conn.from_arrow_table(tbl)
        result = rel.aggregate('sum(a)')
        assert (result.execute().fetchone()[0] == 49999995000000)

        # Single Thread
        duckdb_conn.execute("PRAGMA threads=1")
        assert (result.execute().fetchone()[0] == 49999995000000)

    def test_progress_arrow_empty(self, duckdb_cursor):
        if not can_run:
            return

        data = (pyarrow.array(np.arange(0), type=pyarrow.int32()))
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA set_progress_bar_time=1")
        duckdb_conn.execute("PRAGMA disable_print_progress_bar")

        tbl = pyarrow.Table.from_arrays([data],['a'])
        rel = duckdb_conn.from_arrow_table(tbl)
        result = rel.aggregate('sum(a)')
        assert (result.execute().fetchone()[0] == None)
