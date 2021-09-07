# This script runs in the Regression Test CI.
# We build the current commit and compare it with the Master branch.
# If there is a diference of 10% in regression on any query the build breaks.
import os
import statistics
import time
import subprocess
import math

def truncate(number, decimals=0):
    """
    Returns a value truncated to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer.")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more.")
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor

def install_duck_master():
    os.system("pip install duckdb --pre")
    try:
        import duckdb
    except:
        print ("pip install duckdb --pre FAILED")
        assert(0)

def uninstall_duck():
    duck_installed = True
    while (duck_installed):
        os.system("pip uninstall -y duckdb")
        try:
            import duckdb
        except:
            duck_installed = True


def install_duck_current():
    os.system("BUILD_PYTHON=1 GEN=ninja make release")
    try:
        import duckdb
    except:
        print ("BUILD_PYTHON=1 GEN=ninja make release FAILED")
        assert(0)

def run_tpch_query(duckdb_conn,query_number):
    query_result = []
    for i in range(5):
        query = duckdb_conn.execute("select query from tpch_queries() where query_nr="+str(query_number)).fetchone()[0]
        start_time = time.time()
        result = duckdb_conn.execute(query)
        total_time = time.time() - start_time
        query_result.append(total_time)
    return statistics.median(query_result)

def run_tpch(repetitions):
    import duckdb
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("CALL dbgen(sf=1);")
    result_list = []
    for i in range (1,23):
        query_result_list = []
        for j in range(repetitions):
            query_result_list.append(run_tpch_query(duckdb_conn,i))
        result_list.append(query_result_list)
    return result_list

def run_benchmark(install_function,repetitions):
    install_function()
    result_list = run_tpch(repetitions)
    uninstall_duck()
    return result_list

# We want to run the regression tests 3x if a query fails (i.e., is slower than the one in the master these 3 times then it fails)
def regression_test(threshold):
    uninstall_duck()
    repetitions = 3
    master_time = run_benchmark(install_duck_master,repetitions)
    current_time = run_benchmark(install_duck_current,repetitions)

    print (master_time)
    print(current_time)
    # If the regression status is true, there was no regression
    regression_status = True
    for i in range(len(master_time)):
        # Query Ok means that in all runs at least once it finished below the threshold
        query_ok = False
        # Query Faster means that is always finished faster than the master (using threshold)
        query_faster = True
        for j in range (repetitions):
            if current_time[i][j] <= master_time[i][j] * (1+threshold):
                query_ok = True
            if current_time[i][j] > master_time[i][j] * (1-threshold):
                query_faster = False

        if not query_ok:
                regression_status = False
                print("Q"+ str(i+1) + " slow ("+ str(truncate(current_time[i][0],2)) + " vs " + str(truncate(master_time[i][0],2)) + "). ")
        if query_faster:
                print("Q"+ str(i+1) + " fast ("+ str(truncate(current_time[i][0],2)) + " vs " + str(truncate(master_time[i][0],2)) + "). ")

    if not regression_status:
        assert(0)

regression_test(0.1)