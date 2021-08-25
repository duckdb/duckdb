# This script runs in the Regression Test CI.
# We build the current commit and compare it with the Master branch.
# If there is a diference of 10% in regression on any query the build breaks.
import subprocess
import sys
import os
import statistics
import time
import pip

def install_duck_master():
     pip.main(['install', 'duckdb', '--pre'])

def uninstall_duck():
     pip.main(['uninstall', '-y', 'duckdb'])

def install_duck_current():
    os.system("BUILD_PYTHON=1 GEN=ninja make release")

def run_tpch_query(duckdb_conn,query_number):
    import duckdb
    query_result = []
    for i in range(5):
        query = duckdb_conn.execute("select query from tpch_queries() where query_nr="+str(query_number)).fetchone()[0]
        start_time = time.time()
        result = duckdb_conn.execute(query)
        total_time = time.time() - start_time
        query_result.append(total_time)
    return statistics.median(query_result)

def run_tpch():
    import duckdb
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("CALL dbgen(sf=0.01);")
    result_list = []
    for i in range (1,23):
        result_list.append(run_tpch_query(duckdb_conn,i))
    return result_list

def run_benchmark(install_function):
    install_function()
    result_list = run_tpch()
    uninstall_duck()
    return result_list

def regression_test(threshold):
    master_time = run_benchmark(install_duck_master)
    current_time = run_benchmark(install_duck_current)

    # Print Query Results for Logging
    is_it_faster = True
    for i in range(len(master_time)):
        if current_time[i] > master_time[i] * (1+threshold):
            is_it_faster = False
            print ("Query "+ str(i+1) + " (SLOW) " + "Master: " + str(master_time[i]) + "  Current: " +  str(master_time[i]))
        else:
            print ("Query "+ str(i+1) + " (FAST) " + "Master: " + str(master_time[i]) + "  Current: " +  str(master_time[i]))
    assert (is_it_faster)

regression_test(0.1)