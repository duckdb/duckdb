import os
import sys
import duckdb
import pandas as pd
import pyarrow as pa
import time

threads = None
verbose = False
out_file = None
nruns = 10
TPCH_NQUERIES = 22

for arg in sys.argv[1:]:
    if arg == "--verbose":
        verbose = True
    elif arg.startswith("--threads="):
        threads = int(arg.replace("--threads=", ""))
    elif arg.startswith("--nruns="):
        nruns = int(arg.replace("--nruns=", ""))
    elif arg.startswith("--out-file="):
        out_file = arg.replace("--out-file=", "")
    else:
        print(f"Unrecognized parameter '{arg}'")
        exit(1)

# generate data
if verbose:
    print("Generating TPC-H data")

main_con = duckdb.connect()
main_con.execute('CALL dbgen(sf=1)')

tables = [
 "customer",
 "lineitem",
 "nation",
 "orders",
 "part",
 "partsupp",
 "region",
 "supplier",
]

def open_connection():
    con = duckdb.connect()
    if threads is not None:
        if verbose:
            print(f'Limiting threads to {threads}')
        con.execute(f"SET threads={threads}")
    return con


def write_result(benchmark_name, nrun, t):
    bench_result = f"{benchmark_name}\t{nrun}\t{t}"

    if out_file is not None:
        f.write(bench_result)
        f.write('\n')
    else:
        print(bench_result)

def benchmark_queries(benchmark_name, con, queries):
    if verbose:
        print(benchmark_name)
        print(queries)
    for nrun in range(nruns):
        t = 0.0
        for i, q in enumerate(queries):
            start = time.time()
            df_result = con.execute(q).fetchall()
            end = time.time()
            query_time = float(end - start)
            if verbose:
                print(f"Q{str(i).ljust(len(str(nruns)), ' ')}: {query_time}")
            t += float(end - start)
            if verbose:
                padding = " " * len(str(nruns))
                print(f"T{padding}: {t}s")
        write_result(benchmark_name, nrun, t)

def run_dataload(con, type):
    benchmark_name = type + "_load_lineitem"
    if verbose:
        print(benchmark_name)
        print(type)
    q = 'SELECT * FROM lineitem'
    for nrun in range(nruns):
        t = 0.0
        start = time.time()
        if type == 'pandas':
            res = con.execute(q).df()
        elif type == 'arrow':
            res = con.execute(q).arrow()
        end = time.time()
        t = float(end - start)
        del res
        if verbose:
            padding = " " * len(str(nruns))
            print(f"T{padding}: {t}s")
        write_result(benchmark_name, nrun, t)

def run_tpch(con, prefix):
    benchmark_name = f"{prefix}tpch"
    queries = []
    for i in range(1, TPCH_NQUERIES + 1):
        queries.append(f'PRAGMA tpch({i})')
    benchmark_queries(benchmark_name, con, queries)


if out_file is not None:
    f = open(out_file, 'w+')

# pandas scans
data_frames = {}
for table in tables:
    data_frames[table] = main_con.execute(f"SELECT * FROM {table}").df()

df_con = open_connection()
for table in tables:
    df_con.register(table, data_frames[table])

run_dataload(main_con, "pandas")
run_tpch(df_con, "pandas_")

# arrow scans
arrow_tables = {}
for table in tables:
    arrow_tables[table] = pa.Table.from_pandas(data_frames[table])

arrow_con = open_connection()
for table in tables:
    arrow_con.register(table, arrow_tables[table])

run_dataload(main_con, "arrow")
run_tpch(arrow_con, "arrow_")
