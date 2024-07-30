import numpy as np
import os
import pytest

try:
    import pyarrow as pa

    can_run = True
except ImportError:
    can_run = False
from conftest import NumpyPandas, ArrowPandas


psutil = pytest.importorskip("psutil")


def check_memory(function_to_check, pandas, duckdb_cursor):
    process = psutil.Process(os.getpid())
    mem_usage = process.memory_info().rss / (10**9)
    for __ in range(100):
        function_to_check(pandas, duckdb_cursor)
    cur_mem_usage = process.memory_info().rss / (10**9)
    # This seems a good empirical value
    assert cur_mem_usage / 3 < mem_usage


def from_df(pandas, duckdb_cursor):
    df = pandas.DataFrame({"x": np.random.rand(1_000_000)})
    return duckdb_cursor.from_df(df)


def from_arrow(pandas, duckdb_cursor):
    data = pa.array(np.random.rand(1_000_000), type=pa.float32())
    arrow_table = pa.Table.from_arrays([data], ['a'])
    duckdb_cursor.from_arrow(arrow_table)


def arrow_replacement(pandas, duckdb_cursor):
    data = pa.array(np.random.rand(1_000_000), type=pa.float32())
    arrow_table = pa.Table.from_arrays([data], ['a'])
    duckdb_cursor.query("select sum(a) from arrow_table").fetchall()


def pandas_replacement(pandas, duckdb_cursor):
    df = pandas.DataFrame({"x": np.random.rand(1_000_000)})
    duckdb_cursor.query("select sum(x) from df").fetchall()


class TestRelationDependencyMemoryLeak(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_from_arrow_leak(self, pandas, duckdb_cursor):
        if not can_run:
            return
        check_memory(from_arrow, pandas, duckdb_cursor)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_from_df_leak(self, pandas, duckdb_cursor):
        check_memory(from_df, pandas, duckdb_cursor)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_arrow_replacement_scan_leak(self, pandas, duckdb_cursor):
        if not can_run:
            return
        check_memory(arrow_replacement, pandas, duckdb_cursor)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_pandas_replacement_scan_leak(self, pandas, duckdb_cursor):
        check_memory(pandas_replacement, pandas, duckdb_cursor)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_relation_view_leak(self, pandas, duckdb_cursor):
        rel = from_df(pandas, duckdb_cursor)
        rel.create_view("bla")
        duckdb_cursor.unregister("bla")
        assert rel.query("bla", "select count(*) from bla").fetchone()[0] == 1_000_000
