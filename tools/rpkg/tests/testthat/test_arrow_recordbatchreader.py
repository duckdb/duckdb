import duckdb
import os
try:
    import pyarrow
    import pyarrow.parquet
    import pyarrow.dataset
    import numpy as np
    can_run = True
except:
    can_run = False

class TestArrowRecordBatchReader(object):

    def test_parallel_reader(self,duckdb_cursor):
        if not can_run:
            return

duckdb_conn = duckdb.connect()
duckdb_conn.execute("PRAGMA threads=4")
duckdb_conn.execute("PRAGMA verify_parallelism")

parquet_filename = os.path.join('data','userdata1.parquet')

userdata_parquet_dataset= pyarrow.dataset.dataset([
    parquet_filename,
    parquet_filename,
    parquet_filename,
]
, format="parquet")

batches= [r for r in userdata_parquet_dataset.to_batches()]
reader=pyarrow.dataset.Scanner.from_batches(batches,userdata_parquet_dataset.schema).to_reader()


rel = duckdb_conn.from_arrow_table(reader)

rows=rel.fetchall()

assert rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 12