import psycopg2
import argparse
import os
import platform
import shutil
import sys
import subprocess
import multiprocessing.pool

parser = argparse.ArgumentParser(description='Generate TPC-DS reference results from Postgres.')
parser.add_argument(
    '--sf', dest='sf', action='store', help='The TPC-DS scale factor reference results to generate', default=1
)
parser.add_argument(
    '--query-dir',
    dest='query_dir',
    action='store',
    help='The directory with queries to run',
    default='extension/tpcds/dsdgen/queries',
)
parser.add_argument(
    '--answer-dir',
    dest='answer_dir',
    action='store',
    help='The directory where to store the answers',
    default='extension/tpcds/dsdgen/answers/sf${SF}',
)
parser.add_argument(
    '--duckdb-path',
    dest='duckdb_path',
    action='store',
    help='The path to the DuckDB executable',
    default='build/reldebug/duckdb',
)
parser.add_argument(
    '--skip-load',
    dest='skip_load',
    action='store_const',
    const=True,
    help='Whether or not to skip loading',
    default=False,
)
parser.add_argument(
    '--query-list', dest='query_list', action='store', help='The list of queries to run (default = all)', default=''
)
parser.add_argument('--nthreads', dest='nthreads', action='store', type=int, help='The number of threads', default=0)

args = parser.parse_args()

con = psycopg2.connect(database='postgres')
c = con.cursor()
if not args.skip_load:
    tpcds_dir = f'tpcds_sf{args.sf}'

    q = f"""
    CALL dsdgen(sf={args.sf});
    EXPORT DATABASE '{tpcds_dir}' (DELIMITER '|');
    """
    proc = subprocess.Popen([args.duckdb_path, "-c", q])
    proc.wait()
    if proc.returncode != 0:
        exit(1)

    # drop the previous tables
    tables = [
        'name',
        'web_site',
        'web_sales',
        'web_returns',
        'web_page',
        'warehouse',
        'time_dim',
        'store_sales',
        'store_returns',
        'store',
        'ship_mode',
        'reason',
        'promotion',
        'item',
        'inventory',
        'income_band',
        'household_demographics',
        'date_dim',
        'customer_demographics',
        'customer_address',
        'customer',
        'catalog_sales',
        'catalog_returns',
        'catalog_page',
        'call_center',
    ]
    for table in tables:
        c.execute(f'DROP TABLE IF EXISTS {table};')

    with open(os.path.join(tpcds_dir, 'schema.sql'), 'r') as f:
        schema = f.read()

    c.execute(schema)

    with open(os.path.join(tpcds_dir, 'load.sql'), 'r') as f:
        load = f.read()

    load = load.replace(f'{tpcds_dir}/', f'{os.getcwd()}/{tpcds_dir}/')

    c.execute(load)

    con.commit()

# get a list of all queries
queries = os.listdir(args.query_dir)
queries.sort()

answer_dir = args.answer_dir.replace('${SF}', args.sf)

if len(args.query_list) > 0:
    passing_queries = [x + '.sql' for x in args.query_list.split(',')]
    queries = [x for x in queries if x in passing_queries]
    queries.sort()


def run_query(q):
    print(q)
    with open(os.path.join(args.query_dir, q), 'r') as f:
        sql_query = f.read()
    answer_path = os.path.join(os.getcwd(), answer_dir, q.replace('.sql', '.csv'))
    c.execute(f'DROP TABLE IF EXISTS "query_result{q}"')
    c.execute(f'CREATE TABLE "query_result{q}" AS ' + sql_query)
    c.execute(f"COPY \"query_result{q}\" TO '{answer_path}' (FORMAT CSV, DELIMITER '|', HEADER, NULL 'NULL')")


if args.nthreads == 0:
    for q in queries:
        run_query(q)
else:
    pool = multiprocessing.pool.ThreadPool(processes=args.nthreads)

    pool.map(run_query, queries)
