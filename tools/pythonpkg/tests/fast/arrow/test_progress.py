import duckdb
import os
import pytest

pyarrow_parquet = pytest.importorskip("pyarrow.parquet")
import sys


class TestProgressBarArrow(object):
    def test_progress_arrow(self):
        if os.name == 'nt':
            return
        np = pytest.importorskip("numpy")
        pyarrow = pytest.importorskip("pyarrow")

        data = pyarrow.array(np.arange(10000000), type=pyarrow.int32())
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA progress_bar_time=1")
        duckdb_conn.execute("PRAGMA disable_print_progress_bar")

        tbl = pyarrow.Table.from_arrays([data], ['a'])
        rel = duckdb_conn.from_arrow(tbl)
        result = rel.aggregate('sum(a)')
        assert result.execute().fetchone()[0] == 49999995000000
        # Multiple Threads
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA verify_parallelism")
        assert result.execute().fetchone()[0] == 49999995000000

        # More than one batch
        tbl = pyarrow.Table.from_batches(pyarrow.Table.from_arrays([data], ['a']).to_batches(100))
        rel = duckdb_conn.from_arrow(tbl)
        result = rel.aggregate('sum(a)')
        assert result.execute().fetchone()[0] == 49999995000000

        # Single Thread
        duckdb_conn.execute("PRAGMA threads=1")
        duck_res = result.execute()
        py_res = duck_res.fetchone()[0]
        assert py_res == 49999995000000

    def test_progress_arrow_empty(self):
        if os.name == 'nt':
            return
        np = pytest.importorskip("numpy")
        pyarrow = pytest.importorskip("pyarrow")

        data = pyarrow.array(np.arange(0), type=pyarrow.int32())
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA progress_bar_time=1")
        duckdb_conn.execute("PRAGMA disable_print_progress_bar")

        tbl = pyarrow.Table.from_arrays([data], ['a'])
        rel = duckdb_conn.from_arrow(tbl)
        result = rel.aggregate('sum(a)')
        assert result.execute().fetchone()[0] == None
