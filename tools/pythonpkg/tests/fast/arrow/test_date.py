import duckdb
import os
import datetime
import pytest

try:
    import pyarrow as pa
    import pandas as pd

    can_run = True
except:
    can_run = False


class TestArrowDate(object):
    def test_date_types(self, duckdb_cursor):
        if not can_run:
            return

        data = (pa.array([1000 * 60 * 60 * 24], type=pa.date64()), pa.array([1], type=pa.date32()))
        arrow_table = pa.Table.from_arrays([data[0], data[1]], ['a', 'b'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert rel['a'] == arrow_table['b']
        assert rel['b'] == arrow_table['b']

    def test_date_null(self, duckdb_cursor):
        if not can_run:
            return
        data = (pa.array([None], type=pa.date64()), pa.array([None], type=pa.date32()))
        arrow_table = pa.Table.from_arrays([data[0], data[1]], ['a', 'b'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert rel['a'] == arrow_table['b']
        assert rel['b'] == arrow_table['b']

    def test_max_date(self, duckdb_cursor):
        if not can_run:
            return
        data = (pa.array([2147483647], type=pa.date32()), pa.array([2147483647], type=pa.date32()))
        result = pa.Table.from_arrays([data[0], data[1]], ['a', 'b'])
        data = (
            pa.array([2147483647 * (1000 * 60 * 60 * 24)], type=pa.date64()),
            pa.array([2147483647], type=pa.date32()),
        )
        arrow_table = pa.Table.from_arrays([data[0], data[1]], ['a', 'b'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert rel['a'] == result['a']
        assert rel['b'] == result['b']
