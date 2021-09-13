# This script runs in the Regression Test CI.
# We build the current commit and compare it with the Master branch.
# If there is a diference of 10% in regression on any query the build breaks.
import statistics
import time
import subprocess
import math
import duckdb
import duckcurrent
import sys
import requests

def download_file(url,name):
    r = requests.get(url, allow_redirects=True)
    open(name, 'wb').write(r.content)

def truncate(number, decimals=0):
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer.")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more.")
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor

def run_tpc_query(duckdb_conn,query_number,get_query_sql):
    query_result = []
    query = duckdb_conn.execute("select query from "+get_query_sql +"() where query_nr="+str(query_number)).fetchone()[0]
    for i in range(5):
        start_time = time.time()
        result = duckdb_conn.execute(query)
        total_time = time.time() - start_time
        query_result.append(total_time)
    return statistics.median(query_result)

def run_h2oai_query(duckdb_conn,query_number,queries_list):
    query_result = []
    query = queries_list[query_number-1]
    for i in range(5):
        start_time = time.time()
        result = duckdb_conn.execute(query)
        total_time = time.time() - start_time
        query_result.append(total_time)
        duckdb_conn.execute('DROP TABLE IF EXISTS ans')
    return statistics.median(query_result)

def run_all_queries(duckdb_conn,duckdb_current_conn,benchmark_name,repetitions,num_queries,threshold,query_fun,queries,skip_queries=set([])):
    print ("######## Status " + benchmark_name + " Benchmark Regression #######", flush=True)
    regression_status = True
    for i in range (1,num_queries):      
        if i not in skip_queries:    
            j = 0
            query_faster = True
            query_ok = False
            master_time = 0
            current_time = 0
            # We only repeat the query (up to repetitions), if its not the same result.
            while (j < repetitions and (query_ok is False or query_faster is True)):
                j+=1
                master_time = query_fun(duckdb_conn,i,queries)
                current_time = query_fun(duckdb_current_conn,i,queries)
                if current_time <= master_time * (1+threshold):
                    query_ok = True
                if current_time > master_time * (1-threshold):
                    query_faster = False
            if query_ok is False:
                regression_status = False
                print("Q"+ str(i) + " slow ("+ str(truncate(current_time,2)) + " vs " + str(truncate(master_time,2)) + "). ", flush=True)
            if query_faster:
                print("Q"+ str(i) + " fast ("+ str(truncate(current_time,2)) + " vs " + str(truncate(master_time,2)) + "). ", flush=True)
            if query_ok and not query_faster:
                print("Q"+ str(i) + " same ("+ str(truncate(current_time,2)) + " vs " + str(truncate(master_time,2)) + "). ", flush=True)
    print ("######## End " + benchmark_name + " Benchmark Regression #######", flush=True)
    return regression_status

def run_tpc(benchmark_name,repetitions,load_data_call,num_queries,get_query_sql,threshold,skip_queries=set([])): 
    # Master Branch
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute(load_data_call)
    duckdb_conn.execute('PRAGMA threads=2')
    # This Branch
    duckdb_current_conn = duckcurrent.connect()
    duckdb_current_conn.execute(load_data_call)
    duckdb_current_conn.execute('PRAGMA threads=2')
    return run_all_queries(duckdb_conn,duckdb_current_conn,benchmark_name,repetitions,num_queries,threshold,run_tpc_query,get_query_sql,skip_queries)

def download_h2oai():
    download_file('https://github.com/cwida/duckdb-data/releases/download/v1.0/G1_1e7_1e2_5_0.csv.gz','G1_1e7_1e2_5_0.csv.gz')
    download_file('https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_NA_0_0.csv.gz','J1_1e7_NA_0_0.csv.gz')
    download_file('https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_1e1_0_0.csv.gz','J1_1e7_1e1_0_0.csv.gz')
    download_file('https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_1e4_0_0.csv.gz','J1_1e7_1e4_0_0.csv.gz')
    download_file('https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_1e7_0_0.csv.gz','J1_1e7_1e7_0_0.csv.gz')

def load_h2oai(duck_conn,duckdb_current_conn):
    duck_conn.execute("CREATE TABLE x_group as SELECT * FROM 'G1_1e7_1e2_5_0.csv.gz'")
    duck_conn.execute("CREATE TABLE x as SELECT * FROM 'J1_1e7_NA_0_0.csv.gz'")
    duck_conn.execute("CREATE TABLE small as SELECT * FROM 'J1_1e7_1e1_0_0.csv.gz'")
    duck_conn.execute("CREATE TABLE medium as SELECT * FROM 'J1_1e7_1e4_0_0.csv.gz'")
    duck_conn.execute("CREATE TABLE big as SELECT * FROM 'J1_1e7_1e7_0_0.csv.gz'")

    duckdb_current_conn.execute("CREATE TABLE x_group as SELECT * FROM 'G1_1e7_1e2_5_0.csv.gz'")
    duckdb_current_conn.execute("CREATE TABLE x as SELECT * FROM 'J1_1e7_NA_0_0.csv.gz'")
    duckdb_current_conn.execute("CREATE TABLE small as SELECT * FROM 'J1_1e7_1e1_0_0.csv.gz'")
    duckdb_current_conn.execute("CREATE TABLE medium as SELECT * FROM 'J1_1e7_1e4_0_0.csv.gz'")
    duckdb_current_conn.execute("CREATE TABLE big as SELECT * FROM 'J1_1e7_1e7_0_0.csv.gz'")

def run_h2oai_group(duckdb_conn,duckdb_current_conn,repetitions,threshold):
    queries = ['CREATE TABLE ans AS SELECT id1, sum(v1) AS v1 FROM x_group GROUP BY id1', 
    'CREATE TABLE ans AS SELECT id1, id2, sum(v1) AS v1 FROM x_group GROUP BY id1, id2;',
    'CREATE TABLE ans AS SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM x_group GROUP BY id3;',
    'CREATE TABLE ans AS SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM x_group GROUP BY id4;',
    'CREATE TABLE ans AS SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM x_group GROUP BY id6;',
    'CREATE TABLE ans AS SELECT id4, id5, quantile_cont(v3, 0.5) AS median_v3, stddev(v3) AS sd_v3 FROM x_group GROUP BY id4, id5;',
    'CREATE TABLE ans AS SELECT id3, max(v1)-min(v2) AS range_v1_v2 FROM x_group GROUP BY id3;',
    'CREATE TABLE ans AS SELECT id6, v3 AS largest2_v3 FROM (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 FROM x_group WHERE v3 IS NOT NULL) sub_query WHERE order_v3 <= 2',
    'CREATE TABLE ans AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x_group GROUP BY id2, id4;',
    'CREATE TABLE ans AS SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count(*) AS count FROM x_group GROUP BY id1, id2, id3, id4, id5, id6;']
    
    return run_all_queries(duckdb_conn,duckdb_current_conn,'H2OAI-Group BY', repetitions,10,threshold,run_h2oai_query,queries)

def run_h2oai_join(duckdb_conn,duckdb_current_conn,repetitions,threshold):
    queries = ['CREATE TABLE ans AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1);', 
    'CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2);',
    'CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2);',
    'CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5);',
    'CREATE TABLE ans AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3);']
    
    return run_all_queries(duckdb_conn,duckdb_current_conn,'H2OAI-Join', repetitions,5,threshold,run_h2oai_query,queries)

def run_h2oai(repetitions, threshold):    
    # Master Branch
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute('PRAGMA threads=2')

    # This Branch
    duckdb_current_conn = duckcurrent.connect()
    duckdb_current_conn.execute('PRAGMA threads=2')
    # Download H2OAI data
    download_h2oai()

    # Load the data 
    load_h2oai(duckdb_conn,duckdb_current_conn)

    # Run H2OAI Group
    regression_status = run_h2oai_group(duckdb_conn,duckdb_current_conn,repetitions, threshold)

    # Run H2OAI Join
    regression_status = regression_status and run_h2oai_join(duckdb_conn,duckdb_current_conn,repetitions, threshold)

    return regression_status

def regression_test(threshold,benchmark):
    repetitions = 50 
    regression_status = False
    if benchmark == 'tpch':
        regression_status = run_tpc('TPC-H', repetitions,"CALL dbgen(sf=1);",22,"tpch_queries",threshold)
    elif benchmark == 'tpcds':
        regression_status = run_tpc('TPC-DS', repetitions,"CALL dsdgen(sf=1);",100,"tpcds_queries",threshold,set([64,72,85]))
    elif benchmark == 'h2oai':
        regression_status = run_h2oai(repetitions, threshold)
    else:
        print ("This benchmark does not exist",flush=True)
        assert(0)
    if not regression_status:
        assert(0)

benchmark = ''
for i in range(len(sys.argv)):
    if sys.argv[i].startswith("--benchmark="):
        benchmark = sys.argv[i].split('=', 1)[1]

regression_test(0.15,benchmark)