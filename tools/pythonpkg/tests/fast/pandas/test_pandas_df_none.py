import pandas as pd
import pytest
import duckdb
import sys
import gc


class TestPandasDFNone(object):
    # This used to decrease the ref count of None
    def test_none_deref(self):
        con = duckdb.connect()
        gc.collect()
        for _ in range(1000):
            df = con.sql("select NULL::VARCHAR as a").df()
        gc.collect()
