import duckdb
import os
import sys
import datetime
try:
    import pyarrow as pa
    can_run = True
except:
    can_run = False

class TestArrowReads(object):
    def test_multiple_queries_same_relation(self, duckdb_cursor):
        if not can_run:
            return
        data = (pa.array([datetime.datetime.now()], type=pa.timestamp('ns')),pa.array([datetime.datetime.now()], type=pa.timestamp('us')),pa.array([datetime.datetime.now()], pa.timestamp('ms')),pa.array([datetime.datetime.now()], pa.timestamp('s')))
        print (data)
        arrow_table = pa.Table.from_arrays([data[0],data[1],data[2],data[3]],['a','b','c','d'])
        print (arrow_table)
        rel = duckdb.from_arrow_table(arrow_table).arrow()
        rel.validate(full=True)
        assert rel.equals(arrow_table, check_metadata=True)