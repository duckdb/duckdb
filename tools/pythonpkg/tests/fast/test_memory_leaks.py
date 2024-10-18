import gc
import pytest
import os
import pandas as pd

psutil = pytest.importorskip("psutil")


@pytest.fixture
def check_leaks():
    process = psutil.Process(os.getpid())
    gc.collect()
    initial_mem_usage = process.memory_info().rss
    yield
    gc.collect()
    final_mem_usage = process.memory_info().rss
    difference = final_mem_usage - initial_mem_usage
    print("difference:", difference)
    # Assert that the amount of used memory does not pass 5mb
    assert difference <= 5_000_000


class TestMemoryLeaks(object):
    def test_fetchmany(self, duckdb_cursor, check_leaks):
        datetimes = ['1985-01-30T16:41:43' for _ in range(10000)]

        df = pd.DataFrame({'time': pd.Series(data=datetimes)})
        for _ in range(100):
            duckdb_cursor.sql('select time::TIMESTAMP from df').fetchmany(10000)
