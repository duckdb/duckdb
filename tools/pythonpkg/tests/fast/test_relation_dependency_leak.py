import duckdb
import pandas as pd
import numpy as np
import os, psutil
try:
    import pyarrow as pa
    can_run = True
except:
    can_run = False

def check_memory(function_to_check):
    process = psutil.Process(os.getpid())
    mem_usage = process.memory_info().rss/(10**9)
    for __ in range(100):
        function_to_check()
    cur_mem_usage = process.memory_info().rss/(10**9)
    # This seems a good empirical value
    assert cur_mem_usage/3 < mem_usage

def from_df():
    df = pd.DataFrame({"x": np.random.rand(1_000_000)})
    return duckdb.from_df(df)


def from_arrow():
    data = pa.array(np.random.rand(1_000_000), type=pa.float32())
    arrow_table = pa.Table.from_arrays([data],['a'])
    duckdb.from_arrow(arrow_table)

def arrow_replacement():
    data = pa.array(np.random.rand(1_000_000), type=pa.float32())
    arrow_table = pa.Table.from_arrays([data],['a'])
    duckdb.query("select sum(a) from arrow_table").fetchall()

def pandas_replacement():
    df = pd.DataFrame({"x": np.random.rand(1_000_000)})
    duckdb.query("select sum(x) from df").fetchall()


class TestRelationDependencyMemoryLeak(object):
    def test_from_arrow_leak(self, duckdb_cursor):
        if not can_run:
            return
        check_memory(from_arrow)

    def test_from_df_leak(self, duckdb_cursor):
        check_memory(from_df)

    def test_arrow_replacement_scan_leak(self, duckdb_cursor):
        if not can_run:
            return
        check_memory(arrow_replacement)

    def test_pandas_replacement_scan_leak(self, duckdb_cursor):
        check_memory(pandas_replacement)

    def test_relation_view_leak(self, duckdb_cursor):
        rel = from_df()
        rel.create_view("bla")
        duckdb.default_connection.unregister("bla")
        assert rel.query("bla", "select count(*) from bla").fetchone()[0] == 1_000_000

   
