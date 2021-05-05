import duckdb
import os
import sys
try:
    import pyarrow
    import pyarrow.parquet
    import numpy as np
    can_run = True
except:
    can_run = False

class TestArrowParallel(object):
    def test_parallel_run(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA force_parallelism")
        data = (pyarrow.array(np.random.randint(800, size=1000000), type=pyarrow.int32()))
        tbl = pyarrow.Table.from_batches(pyarrow.Table.from_arrays([data],['a']).to_batches(10000))
        rel = duckdb.from_arrow_table(tbl)
        # Also test multiple reads
        assert(rel.aggregate("(count(a))::INT").execute().fetchone()[0] == 1000000)
        assert(rel.aggregate("(count(a))::INT").execute().fetchone()[0] == 1000000)