import duckdb
import os
import sys
import datetime
try:
    import pyarrow as pa
    can_run = True
except:
    can_run = False

class TestArrowTimestamps(object):
    def test_timestamp_types(self, duckdb_cursor):
        if not can_run:
            return
        data = (pa.array([datetime.datetime.now()], type=pa.timestamp('ns')),pa.array([datetime.datetime.now()], type=pa.timestamp('us')),pa.array([datetime.datetime.now()], pa.timestamp('ms')),pa.array([datetime.datetime.now()], pa.timestamp('s')))
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2],data[3]],['a','b','c','d'])
        rel = duckdb.from_arrow_table(arrow_table).arrow()
        assert (rel['a'] == arrow_table['a'])
        assert (rel['b'] == arrow_table['b'])
        assert (rel['c'] == arrow_table['c'])
        assert (rel['d'] == arrow_table['d'])
