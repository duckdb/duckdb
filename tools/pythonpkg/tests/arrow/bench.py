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

batch_sizes = [1024, 100000, 1000000]

num_threads = [8,16,32,64,128]

for batch in batch_sizes:
    print ("Batch :" + str(batch))
    print ("Thread : 1" )
    data = (pyarrow.array(np.random.randint(800, size=100000000), type=pyarrow.int32()))
    tbl = pyarrow.Table.from_batches(pyarrow.Table.from_arrays([data],['a']).to_batches(batch))
    rel = duckdb_conn.from_arrow_table(tbl)
    duckdb_conn.execute("PRAGMA threads=1")

    start_time = time.time()
    result = rel.aggregate("(count(a))::INT").execute().fetchone()[0]

    time_arrow_to_duck = time.time() - start_time
        # assert (result == 100000)
    print (time_arrow_to_duck)
    for thread in num_threads:
        print ("Thread : "+str(thread))
        data = (pyarrow.array(np.random.randint(800, size=100000000), type=pyarrow.int32()))
        tbl = pyarrow.Table.from_batches(pyarrow.Table.from_arrays([data],['a']).to_batches(batch))
        rel = duckdb_conn.from_arrow_table(tbl)

        duckdb_conn.execute("PRAGMA threads="+str(thread))
        duckdb_conn.execute("PRAGMA force_parallelism")

        start_time = time.time()
        result = rel.aggregate("(count(a))::INT").execute().fetchone()[0]
        time_arrow_to_duck = time.time() - start_time

        print (time_arrow_to_duck)
# assert (result == 100000)