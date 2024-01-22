import pandas as pd
import pytest
import duckdb
import sys
import gc


class TestPandasDFNone(object):
    # This used to decrease the ref count of None
    def test_none_deref(self):
        con = duckdb.connect()
        df = con.sql("select NULL::VARCHAR as a from range(1000000)").df()
