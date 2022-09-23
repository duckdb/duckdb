import duckdb
import os
try:
    import pyarrow as pa
    from pyarrow import parquet as pq
    import numpy as np
    can_run = True
except:
    can_run = False

class TestArrowLargeString(object):
    def test_large_string_type(self,duckdb_cursor):
        if not can_run:
            return
            
        schema = pa.schema([("data", pa.large_string())])
        inputs = [pa.array(["foo", "baaaar", "b"], type=pa.large_string())]
        arrow_table = pa.Table.from_arrays(inputs, schema=schema)

        rel = duckdb.from_arrow(arrow_table)
        res = rel.execute().fetchall()
        assert res == [('foo',), ('baaaar',), ('b',)]
            