import duckdb
import numpy as np
import os, psutil
import pytest

try:
    import pyarrow as pa

    can_run = True
except:
    can_run = False
from conftest import NumpyPandas, ArrowPandas


def check_memory(function_to_check, pandas):
    process = psutil.Process(os.getpid())
    mem_usage = process.memory_info().rss / (10**9)
    for __ in range(100):
        function_to_check(pandas)
    cur_mem_usage = process.memory_info().rss / (10**9)
    # This seems a good empirical value
    assert cur_mem_usage / 3 < mem_usage


def from_df(pandas):
    df = pandas.DataFrame({"x": np.random.rand(1_000_000)})
    return duckdb.from_df(df)


def from_arrow(pandas):
    data = pa.array(np.random.rand(1_000_000), type=pa.float32())
    arrow_table = pa.Table.from_arrays([data], ['a'])
    duckdb.from_arrow(arrow_table)


def arrow_replacement(pandas):
    data = pa.array(np.random.rand(1_000_000), type=pa.float32())
    arrow_table = pa.Table.from_arrays([data], ['a'])
    duckdb.query("select sum(a) from arrow_table").fetchall()


def pandas_replacement(pandas):
    df = pandas.DataFrame({"x": np.random.rand(1_000_000)})
    duckdb.query("select sum(x) from df").fetchall()


class TestRelationDependencyMemoryLeak(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_from_arrow_leak(self, duckdb_cursor, pandas):
        if not can_run:
            return
        check_memory(from_arrow, pandas)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_from_df_leak(self, duckdb_cursor, pandas):
        check_memory(from_df, pandas)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_arrow_replacement_scan_leak(self, duckdb_cursor, pandas):
        if not can_run:
            return
        check_memory(arrow_replacement, pandas)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_pandas_replacement_scan_leak(self, duckdb_cursor, pandas):
        check_memory(pandas_replacement, pandas)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_relation_view_leak(self, duckdb_cursor, pandas):
        rel = from_df(pandas)
        rel.create_view("bla")
        duckdb.default_connection.unregister("bla")
        assert rel.query("bla", "select count(*) from bla").fetchone()[0] == 1_000_000
