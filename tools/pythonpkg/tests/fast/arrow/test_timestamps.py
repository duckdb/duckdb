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

class TestArrowTimestamps(object):
    def test_timestamp_types(self, duckdb_cursor):
        if not can_run:
            return
        data = (pa.array([datetime.datetime.now()], type=pa.timestamp('ns')),pa.array([datetime.datetime.now()], type=pa.timestamp('us')),pa.array([datetime.datetime.now()], pa.timestamp('ms')),pa.array([datetime.datetime.now()], pa.timestamp('s')))
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2],data[3]],['a','b','c','d'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert (rel['a'] == arrow_table['a'])
        assert (rel['b'] == arrow_table['b'])
        assert (rel['c'] == arrow_table['c'])
        assert (rel['d'] == arrow_table['d'])

    def test_timestamp_nulls(self, duckdb_cursor):
        if not can_run:
            return
        data = (pa.array([None], type=pa.timestamp('ns')),pa.array([None], type=pa.timestamp('us')),pa.array([None], pa.timestamp('ms')),pa.array([None], pa.timestamp('s')))
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2],data[3]],['a','b','c','d'])
        rel = duckdb.from_arrow(arrow_table).arrow()
        assert (rel['a'] == arrow_table['a'])
        assert (rel['b'] == arrow_table['b'])
        assert (rel['c'] == arrow_table['c'])
        assert (rel['d'] == arrow_table['d'])

    def test_timestamp_overflow(self, duckdb_cursor):
        if not can_run:
            return
        data = (pa.array([9223372036854775807], pa.timestamp('s')),pa.array([9223372036854775807], pa.timestamp('ms')),pa.array([9223372036854775807], pa.timestamp('us')))
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2]],['a','b','c'])
        arrow_from_duck = duckdb.from_arrow(arrow_table).arrow()
        assert (arrow_from_duck['a'] == arrow_table['a'])
        assert (arrow_from_duck['b'] == arrow_table['b'])
        assert (arrow_from_duck['c'] == arrow_table['c'])

               

        with pytest.raises(Exception):
            duck_rel = duckdb.from_arrow(arrow_table)
            res = duck_rel.project('a::TIMESTAMP_US')
            res.fetchone()

        with pytest.raises(Exception):
            duck_rel = duckdb.from_arrow(arrow_table)
            res = duck_rel.project('b::TIMESTAMP_US')
            res.fetchone()

        with pytest.raises(Exception):
            duck_rel = duckdb.from_arrow(arrow_table)
            res = duck_rel.project('c::TIMESTAMP_NS')
            res.fetchone()
       