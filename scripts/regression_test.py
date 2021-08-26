# This script runs in the Regression Test CI.
# We build the current commit and compare it with the Master branch.
# If there is a diference of 10% in regression on any query the build breaks.
import os
import statistics
import time
import subprocess

def install_duck_master():
    os.system("pip install duckdb --pre")

def uninstall_duck():
    os.system("pip uninstall -y duckdb")

def install_duck_current():
    os.system("BUILD_PYTHON=1 GEN=ninja make release")

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
    duckdb_conn.execute("CALL dbgen(sf=0.01);")
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
    repetitions = 3
    master_time = run_benchmark(install_duck_master,repetitions)
    current_time = run_benchmark(install_duck_current,repetitions)

    # If the regression status is true, there was no regression
    regression_status = True
    description = 'test'
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
                description += "Q"+ str(i+1) + " is slower ("+ str(current_time[i][0]) + "vs" + str(master_time[i][0]) + "). "
        if query_faster:
                description += "Q"+ str(i+1) + " is faster ("+ str(current_time[i][0]) + "vs" + str(master_time[i][0]) + "). "

        if regression_status:
            os.environ["REGRESSION_STATE"] = "success"
        else:
            os.environ["REGRESSION_STATE"] = "failure"

        os.environ["REGRESSION_DESCRIPTION"] = description
        exp = 'export REGRESSION_DESCRIPTION="youAsWell"'
        subprocess.Popen(exp, shell=True).wait()

regression_test(0.1)