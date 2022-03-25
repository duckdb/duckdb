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

class TestArrowInterval(object):
    def test_duration_types(self, duckdb_cursor):
        if not can_run:
            return
        data = (pa.array([1000000000], type=pa.duration('ns')),pa.array([1000000], type=pa.duration('us')),pa.array([1000], pa.duration('ms')),pa.array([1], pa.duration('s')))
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2],data[3]],['a','b','c','d'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert (rel['a'] == arrow_table['c'])
        assert (rel['b'] == arrow_table['c'])
        assert (rel['c'] == arrow_table['c'])
        assert (rel['d'] == arrow_table['c'])

    def test_duration_null(self, duckdb_cursor):
        if not can_run:
            return   
        data = (pa.array([None], type=pa.duration('ns')),pa.array([None], type=pa.duration('us')),pa.array([None], pa.duration('ms')),pa.array([None], pa.duration('s')))
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2],data[3]],['a','b','c','d'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert (rel['a'] == arrow_table['c'])
        assert (rel['b'] == arrow_table['c'])
        assert (rel['c'] == arrow_table['c'])
        assert (rel['d'] == arrow_table['c'])


    def test_duration_overflow(self, duckdb_cursor):
        if not can_run:
            return

        # Only seconds can overflow
        data = pa.array([9223372036854775807], pa.duration('s'))
        arrow_table = pa.Table.from_arrays([data],['a'])

        with pytest.raises(Exception):
             arrow_from_duck = duckdb.from_arrow(arrow_table).arrow()
       