import duckdb
import os
try:
    import pyarrow as pa
    from pyarrow import parquet as pq
    import numpy as np
    can_run = True
except:
    can_run = False

def create_binary_table(type):
    schema = pa.schema([("data", type)])
    inputs = [pa.array([b"foo", b"bar", b"baz"], type=type)]
    return pa.Table.from_arrays(inputs, schema=schema)

class TestArrowBinary(object):
    def test_binary_types(self,duckdb_cursor):
        if not can_run:
            return

        # Fixed Size Binary
        arrow_table = create_binary_table(pa.binary(3))
        rel = duckdb.from_arrow(arrow_table)
        res = rel.execute().fetchall()
        assert res == [(b"foo",), (b"bar",), (b"baz",)]

        # Normal Binary
        arrow_table = create_binary_table(pa.binary())
        rel = duckdb.from_arrow(arrow_table)
        res = rel.execute().fetchall()
        assert res == [(b"foo",), (b"bar",), (b"baz",)]

        # Large Binary
        arrow_table = create_binary_table(pa.large_binary())
        rel = duckdb.from_arrow(arrow_table)
        res = rel.execute().fetchall()
        assert res == [(b"foo",), (b"bar",), (b"baz",)]

    