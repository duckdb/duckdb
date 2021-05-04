import duckdb
import os
import sys
import time
try:
    import pyarrow
    import pyarrow.parquet
    import numpy as np
    can_run = True
except:
    can_run = False

duckdb_conn = duckdb.connect()

        
data = (pyarrow.array(np.random.randint(800, size=100000000), type=pyarrow.int32()))
tbl = pyarrow.Table.from_batches(pyarrow.Table.from_arrays([data],['a']).to_batches(1000000))
rel = duckdb_conn.from_arrow_table(tbl)
duckdb_conn.execute("PRAGMA threads=1")

start_time = time.time()
result = rel.aggregate("(count(a))::INT").execute().fetchone()[0]

time_arrow_to_duck = time.time() - start_time
# assert (result == 100000)
print (result)
print (time_arrow_to_duck)

data = (pyarrow.array(np.random.randint(800, size=100000000), type=pyarrow.int32()))
tbl = pyarrow.Table.from_batches(pyarrow.Table.from_arrays([data],['a']).to_batches(1000000))
rel = duckdb_conn.from_arrow_table(tbl)

duckdb_conn.execute("PRAGMA threads=8")
duckdb_conn.execute("PRAGMA force_parallelism")

start_time = time.time()
result = rel.aggregate("(count(a))::INT").execute().fetchone()[0]
time_arrow_to_duck = time.time() - start_time

print (time_arrow_to_duck)
print (result)
# assert (result == 100000)