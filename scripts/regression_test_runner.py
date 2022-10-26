import os
import sys
import subprocess
from io import StringIO
import csv
import statistics

# how many times we will run the experiment, to be sure of the regression
number_repetitions = 5
# the threshold at which we consider something a regression (percentage)
regression_threshold_percentage = 0.1
# minimal seconds diff for something to be a regression (for very fast benchmarks)
regression_threshold_seconds = 0.01

old_runner = None
new_runner = None
benchmark_file = None
verbose = False
threads = None
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

if old_runner is None or new_runner is None or benchmark_file is None:
    print("Expected usage: python3 scripts/regression_test_runner.py --old=/old/benchmark_runner --new=/new/benchmark_runner --benchmarks=/benchmark/list.csv")
    exit(1)

if not os.path.isfile(old_runner):
    print(f"Failed to find old runner {old_runner}")
    exit(1)

if not os.path.isfile(new_runner):
    print(f"Failed to find new runner {new_runner}")
    exit(1)

def run_benchmark(runner, benchmark):
    benchmark_args = [runner, benchmark]
    if threads is not None:
        benchmark_args += ["--threads=%d" % (threads,)]
    proc = subprocess.Popen(benchmark_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = proc.stdout.read().decode('utf8')
    err = proc.stderr.read().decode('utf8')
    proc.wait()
    if proc.returncode != 0:
        print("Failed to run benchmark " + benchmark)
        print('''====================================================
==============         STDERR          =============
====================================================
''')
        print(err)
        print('''====================================================
==============         STDOUT          =============
====================================================
''')
        print(out)
        exit(1)
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
    except:
        print("Failed to run benchmark " + benchmark)
        print(err)
        exit(1)
    return float(statistics.median(timings))

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
for i in range(number_repetitions):
    regression_list = []
    if len(benchmark_list) == 0:
        break
    print(f'''====================================================
==============      ITERATION {i}        =============
==============      REMAINING {len(benchmark_list)}        =============
====================================================
''')

    old_results = run_benchmarks(old_runner, benchmark_list)
    new_results = run_benchmarks(new_runner, benchmark_list)

    for benchmark in benchmark_list:
        old_res = old_results[benchmark]
        new_res = new_results[benchmark]
        if (old_res + regression_threshold_seconds) * multiply_percentage < new_res:
            regression_list.append([benchmark, old_res, new_res])
        else:
            other_results.append([benchmark, old_res, new_res])
    benchmark_list = [x[0] for x in regression_list]

exit_code = 0
if len(regression_list) > 0:
    exit_code = 1
    print('''====================================================
==============  REGRESSIONS DETECTED   =============
====================================================
''')
    for regression in regression_list:
        print(f"{regression[0]}")
        print(f"Old timing: {regression[1]}")
        print(f"New timing: {regression[2]}")
        print("")
    print('''====================================================
==============     OTHER TIMINGS       =============
====================================================
''')
else:
    print('''====================================================
============== NO REGRESSIONS DETECTED  =============
====================================================
''')

other_results.sort()
for res in other_results:
    print(f"{res[0]}")
    print(f"Old timing: {res[1]}")
    print(f"New timing: {res[2]}")
    print("")

exit(exit_code)
