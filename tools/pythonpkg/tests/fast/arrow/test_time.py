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

class TestArrowTime(object):
    def test_time_types(self, duckdb_cursor):
        if not can_run:
            return
        
        data = (pa.array([1], type=pa.time32('s')),pa.array([1000], type=pa.time32('ms')),pa.array([1000000], pa.time64('us')),pa.array([1000000000], pa.time64('ns')))
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2],data[3]],['a','b','c','d'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert (rel['a'] == arrow_table['c'])
        assert (rel['b'] == arrow_table['c'])
        assert (rel['c'] == arrow_table['c'])
        assert (rel['d'] == arrow_table['c'])


    def test_time_null(self, duckdb_cursor):
        if not can_run:
            return   
        data = (pa.array([None], type=pa.time32('s')),pa.array([None], type=pa.time32('ms')),pa.array([None], pa.time64('us')),pa.array([None], pa.time64('ns')))
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2],data[3]],['a','b','c','d'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert (rel['a'] == arrow_table['c'])
        assert (rel['b'] == arrow_table['c'])
        assert (rel['c'] == arrow_table['c'])
        assert (rel['d'] == arrow_table['c'])

    def test_max_times(self, duckdb_cursor):
        if not can_run:
            return   
        data = pa.array([2147483647000000], type=pa.time64('us'))
        result = pa.Table.from_arrays([data],['a'])
        #Max Sec
        data = pa.array([2147483647], type=pa.time32('s'))
        arrow_table = pa.Table.from_arrays([data],['a'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert (rel['a'] == result['a'])

        #Max MSec
        data = pa.array([2147483647000], type=pa.time64('us'))
        result = pa.Table.from_arrays([data],['a'])
        data = pa.array([2147483647], type=pa.time32('ms'))
        arrow_table = pa.Table.from_arrays([data],['a'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert (rel['a'] == result['a'])

        #Max NSec
        data = pa.array([9223372036854774], type=pa.time64('us'))
        result = pa.Table.from_arrays([data],['a'])
        data = pa.array([9223372036854774000], type=pa.time64('ns'))
        arrow_table = pa.Table.from_arrays([data],['a'])
        rel = duckdb.from_arrow(arrow_table).arrow()

        print (rel['a'])
        print (result['a'])
        assert (rel['a'] == result['a'])