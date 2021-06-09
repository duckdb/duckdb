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
        rel = duckdb_conn.from_arrow_table(tbl)
        # Also test multiple reads
        assert(rel.aggregate("(count(a))::INT").execute().fetchone()[0] == 1000000)
        assert(rel.aggregate("(count(a))::INT").execute().fetchone()[0] == 1000000)

    def test_parallel_types_and_different_batches(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA force_parallelism")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')
        cols = 'id, first_name, last_name, email, gender, ip_address, cc, country, birthdate, salary, title, comments'

        userdata_parquet_table = pyarrow.parquet.read_table(parquet_filename)
        for i in [7, 51, 99, 100, 101, 500, 1000, 2000]:
            data = (pyarrow.array(np.arange(3,7), type=pyarrow.int32()))
            tbl = pyarrow.Table.from_arrays([data],['a'])
            rel_id = duckdb_conn.from_arrow_table(tbl)
            userdata_parquet_table2 = pyarrow.Table.from_batches(userdata_parquet_table.to_batches(i))
            rel = duckdb_conn.from_arrow_table(userdata_parquet_table2)
            result = rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)')
            assert (result.execute().fetchone()[0] == 4)

    def test_parallel_fewer_batches_than_threads(self,duckdb_cursor):
        if not can_run:
            return
        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")
        duckdb_conn.execute("PRAGMA force_parallelism")

        data = (pyarrow.array(np.random.randint(800, size=1000), type=pyarrow.int32()))
        tbl = pyarrow.Table.from_batches(pyarrow.Table.from_arrays([data],['a']).to_batches(2))
        rel = duckdb_conn.from_arrow_table(tbl)
        # Also test multiple reads
        assert(rel.aggregate("(count(a))::INT").execute().fetchone()[0] == 1000)