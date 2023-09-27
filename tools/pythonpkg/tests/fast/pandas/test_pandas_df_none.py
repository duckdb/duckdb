import pandas as pd
import pytest
import duckdb
import sys
import gc


class TestPandasDFNone(object):
    def test_none_deref(self):
        con = duckdb.connect()
        gc.collect()
        none_refcount = sys.getrefcount(None)
        for _ in range(1000):
            df = con.sql("select NULL::VARCHAR as a").df()

        gc.collect()
        after = sys.getrefcount(None)
        # Verify that we don't deref None when creating a Pandas DataFrame
        assert after >= none_refcount
