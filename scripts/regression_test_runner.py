import os
import sys
import subprocess
from io import StringIO
import csv
import statistics
import math
import functools
import shutil

print = functools.partial(print, flush=True)


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


# Geometric mean of an array of numbers
def geomean(xs):
    if len(xs) == 0:
        return 'EMPTY'
    for entry in xs:
        if not is_number(entry):
            return entry
    return math.exp(math.fsum(math.log(float(x)) for x in xs) / len(xs))


# how many times we will run the experiment, to be sure of the regression
number_repetitions = 5
# the threshold at which we consider something a regression (percentage)
regression_threshold_percentage = 0.1
# minimal seconds diff for something to be a regression (for very fast benchmarks)
regression_threshold_seconds = 0.05

old_runner = None
new_runner = None
benchmark_file = None
verbose = False
threads = None
no_regression_fail = False
disable_timeout = False
max_timeout = 3600
root_dir = ""
for arg in sys.argv:
    if arg.startswith("--old="):
        old_runner = arg.replace("--old=", "")
    elif arg.startswith("--new="):
        new_runner = arg.replace("--new=", "")
    elif arg.startswith("--benchmarks="):
        benchmark_file = arg.replace("--benchmarks=", "")
    elif arg == "--verbose":
        verbose = True
    elif arg.startswith("--threads="):
        threads = int(arg.replace("--threads=", ""))
    elif arg.startswith("--nofail"):
        no_regression_fail = True
    elif arg == "--disable-timeout":
        disable_timeout = True
    elif arg.startswith("--root-dir="):
        root_dir = arg.replace("--root-dir=", "")

if old_runner is None or new_runner is None or benchmark_file is None:
    print(
        "Expected usage: python3 scripts/regression_test_runner.py --old=/old/benchmark_runner --new=/new/benchmark_runner --benchmarks=/benchmark/list.csv"
    )
    exit(1)

if not os.path.isfile(old_runner):
    print(f"Failed to find old runner {old_runner}")
    exit(1)

if not os.path.isfile(new_runner):
    print(f"Failed to find new runner {new_runner}")
    exit(1)

complete_timings = {old_runner: [], new_runner: []}


def run_benchmark(runner, benchmark):
    benchmark_args = [runner, benchmark]

    if root_dir:
        benchmark_args += [f"--root-dir"]
        benchmark_args += [root_dir]

    if threads is not None:
        benchmark_args += ["--threads=%d" % (threads,)]
    if disable_timeout:
        benchmark_args += ["--disable-timeout"]
        timeout_seconds = max_timeout
    else:
        timeout_seconds = 600
    try:
        proc = subprocess.run(benchmark_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout_seconds)
        out = proc.stdout.decode('utf8')
        err = proc.stderr.decode('utf8')
        returncode = proc.returncode
    except subprocess.TimeoutExpired:
        print("Failed to run benchmark " + benchmark)
        print(f"Aborted due to exceeding the limit of {timeout_seconds} seconds")
        return 'Failed to run benchmark ' + benchmark
    if returncode != 0:
        print("Failed to run benchmark " + benchmark)
        print(
            '''====================================================
==============         STDERR          =============
====================================================
'''
        )
        print(err)
        print(
            '''====================================================
==============         STDOUT          =============
====================================================
'''
        )
        print(out)
        if 'HTTP' in err:
            print("Ignoring HTTP error and terminating the running of the regression tests")
            exit(0)
        return 'Failed to run benchmark ' + benchmark
    if verbose:
        print(err)
    # read the input CSV
    f = StringIO(err)
    csv_reader = csv.reader(f, delimiter='\t')
    header = True
    timings = []
    try:
        for row in csv_reader:
            if len(row) == 0:
                continue
            if header:
                header = False
            else:
                timings.append(row[2])
                complete_timings[runner].append(row[2])
        return float(statistics.median(timings))
    except:
        print("Failed to run benchmark " + benchmark)
        print(err)
        return 'Failed to run benchmark ' + benchmark


def run_benchmarks(runner, benchmark_list):
    results = {}
    for benchmark in benchmark_list:
        results[benchmark] = run_benchmark(runner, benchmark)
    return results


# read the initial benchmark list
with open(benchmark_file, 'r') as f:
    benchmark_list = [x.strip() for x in f.read().split('\n') if len(x) > 0]

multiply_percentage = 1.0 + regression_threshold_percentage
other_results = []
error_list = []
for i in range(number_repetitions):
    regression_list = []
    if len(benchmark_list) == 0:
        break
    print(
        f'''====================================================
==============      ITERATION {i}        =============
==============      REMAINING {len(benchmark_list)}        =============
====================================================
'''
    )

    old_results = run_benchmarks(old_runner, benchmark_list)
    new_results = run_benchmarks(new_runner, benchmark_list)

    for benchmark in benchmark_list:
        old_res = old_results[benchmark]
        new_res = new_results[benchmark]
        if isinstance(old_res, str) or isinstance(new_res, str):
            # benchmark failed to run - always a regression
            error_list.append([benchmark, old_res, new_res])
        elif (no_regression_fail == False) and (
            (old_res + regression_threshold_seconds) * multiply_percentage < new_res
        ):
            regression_list.append([benchmark, old_res, new_res])
        else:
            other_results.append([benchmark, old_res, new_res])
    benchmark_list = [x[0] for x in regression_list]

exit_code = 0
regression_list += error_list
if len(regression_list) > 0:
    exit_code = 1
    print(
        '''====================================================
==============  REGRESSIONS DETECTED   =============
====================================================
'''
    )
    for regression in regression_list:
        print(f"{regression[0]}")
        print(f"Old timing: {regression[1]}")
        print(f"New timing: {regression[2]}")
        print("")
    print(
        '''====================================================
==============     OTHER TIMINGS       =============
====================================================
'''
    )
else:
    print(
        '''====================================================
============== NO REGRESSIONS DETECTED  =============
====================================================
'''
    )

other_results.sort()
for res in other_results:
    print(f"{res[0]}")
    print(f"Old timing: {res[1]}")
    print(f"New timing: {res[2]}")
    print("")

time_a = geomean(complete_timings[old_runner])
time_b = geomean(complete_timings[new_runner])


print("")
if isinstance(time_a, str) or isinstance(time_b, str):
    print(f"Old: {time_a}")
    print(f"New: {time_b}")
elif time_a > time_b * 1.01:
    print(f"Old timing geometric mean: {time_a}")
    print(f"New timing geometric mean: {time_b}, roughly {int((time_a - time_b) * 100.0 / time_a)}% faster")
elif time_b > time_a * 1.01:
    print(f"Old timing geometric mean: {time_a}, roughly {int((time_b - time_a) * 100.0 / time_b)}% faster")
    print(f"New timing geometric mean: {time_b}")
else:
    print(f"Old timing geometric mean: {time_a}")
    print(f"New timing geometric mean: {time_b}")

# nuke cached benchmark data between runs
if os.path.isdir("duckdb_benchmark_data"):
    shutil.rmtree('duckdb_benchmark_data')
exit(exit_code)
