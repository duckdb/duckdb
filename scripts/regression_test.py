# This script runs in the Regression Test CI.
# We build the current commit and compare it with the Master branch.
# If there is a diference of 10% in regression on any query the build breaks.
import statistics
import time
import subprocess
import math
import duckdb
import duckcurrent

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
    for i in range(5):
        query = duckdb_conn.execute("select query from "+get_query_sql +"() where query_nr="+str(query_number)).fetchone()[0]
        start_time = time.time()
        result = duckdb_conn.execute(query)
        total_time = time.time() - start_time
        query_result.append(total_time)
    return statistics.median(query_result)

def run_tpc(repetitions,load_data_call,num_queries,get_query_sql,benchmark_name,threshold,skip_queries=set([])):
    print ("######## Status " + benchmark_name + " Benchmark Regression #######", flush=True)
    regression_status = True
    
    # Master Branch
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute(load_data_call)

    # This Branch
    duckdb_current_conn = duckcurrent.connect()
    duckdb_current_conn.execute(load_data_call)

    for i in range (1,num_queries):      
        start_time = time.time()
        if i not in skip_queries:    
            query_result_list = []
            j = 0
            query_faster = True
            query_ok = False
            master_time = 0
            current_time = 0
            # We only repeat the query (up to repetitions), if its not the same result.
            while (j < repetitions and query_ok is False):
                j+=1
                master_time = run_tpc_query(duckdb_conn,i,get_query_sql)
                current_time = run_tpc_query(duckdb_current_conn,i,get_query_sql)
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

def regression_test(threshold):
    repetitions = 100 
    # Run TPC-H
    regression_status_tpch = run_tpc(repetitions,"CALL dbgen(sf=1);",22,"tpch_queries","TPC-H",threshold)
    # Run TPC-DS
    regression_status_tpcds = run_tpc(repetitions,"CALL dsdgen(sf=1);",100,"tpcds_queries","TPC-DS",threshold,set([64,72,85]))
    regression_status = regression_status_tpch and regression_status_tpcds
    if not regression_status:
        assert(0)

regression_test(0.15)