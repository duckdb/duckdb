import os
import math
import functools
import shutil
from benchmark import BenchmarkRunner, BenchmarkRunnerConfig
from dataclasses import dataclass
from typing import Optional, List

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
NUMBER_REPETITIONS = 5
# the threshold at which we consider something a regression (percentage)
REGRESSION_THRESHOLD_PERCENTAGE = 0.1
# minimal seconds diff for something to be a regression (for very fast benchmarks)
REGRESSION_THRESHOLD_SECONDS = 0.05

import argparse

# Set up the argument parser
parser = argparse.ArgumentParser(description="Benchmark script with old and new runners.")

# Define the arguments
parser.add_argument("--old", type=str, help="Path to the old runner.", required=True)
parser.add_argument("--new", type=str, help="Path to the new runner.", required=True)
parser.add_argument("--benchmarks", type=str, help="Path to the benchmark file.", required=True)
parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
parser.add_argument("--threads", type=int, help="Number of threads to use.")
parser.add_argument("--nofail", action="store_true", help="Do not fail on regression.")
parser.add_argument("--disable-timeout", action="store_true", help="Disable timeout.")
parser.add_argument("--max-timeout", type=int, default=3600, help="Set maximum timeout in seconds (default: 3600).")
parser.add_argument("--root-dir", type=str, default="", help="Root directory.")

# Parse the arguments
args = parser.parse_args()

# Assign parsed arguments to variables
old_runner_path = args.old
new_runner_path = args.new
benchmark_file = args.benchmarks
verbose = args.verbose
threads = args.threads
no_regression_fail = args.nofail
disable_timeout = args.disable_timeout
max_timeout = args.max_timeout
root_dir = args.root_dir

if not os.path.isfile(old_runner_path):
    print(f"Failed to find old runner {old_runner_path}")
    exit(1)

if not os.path.isfile(new_runner_path):
    print(f"Failed to find new runner {new_runner_path}")
    exit(1)

config_dict = vars(args)
old_runner = BenchmarkRunner(BenchmarkRunnerConfig.from_params(old_runner_path, benchmark_file, **config_dict))
new_runner = BenchmarkRunner(BenchmarkRunnerConfig.from_params(new_runner_path, benchmark_file, **config_dict))

benchmark_list = old_runner.benchmark_list


@dataclass
class BenchmarkResult:
    benchmark: str
    old_result: Optional[float] = None
    old_error: Optional[str] = None
    new_result: Optional[float] = None
    new_error: Optional[str] = None


multiply_percentage = 1.0 + REGRESSION_THRESHOLD_PERCENTAGE
other_results: List[BenchmarkResult] = []
error_list: List[BenchmarkResult] = []
for i in range(NUMBER_REPETITIONS):
    regression_list: List[BenchmarkResult] = []
    if len(benchmark_list) == 0:
        break
    print(
        f'''====================================================
==============      ITERATION {i}        =============
==============      REMAINING {len(benchmark_list)}        =============
====================================================
'''
    )

    old_results = old_runner.run_benchmarks()
    new_results = new_runner.run_benchmarks()

    for benchmark in benchmark_list:
        old_res = old_results[benchmark]
        new_res = new_results[benchmark]
        if isinstance(old_res, str) or isinstance(new_res, str):
            # benchmark failed to run - always a regression
            error_list.append(BenchmarkResult(benchmark, old_error=old_res, new_error=new_res))
        elif (no_regression_fail == False) and (
            (old_res + REGRESSION_THRESHOLD_SECONDS) * multiply_percentage < new_res
        ):
            regression_list.append(BenchmarkResult(benchmark, old_result=old_res, new_result=new_res))
        else:
            other_results.append(BenchmarkResult(benchmark, old_result=old_res, new_result=new_res))
    benchmark_list = [res.benchmark for res in regression_list]

exit_code = 0
regression_list.extend(error_list)
if len(regression_list) > 0:
    exit_code = 1
    print(
        '''====================================================
==============  REGRESSIONS DETECTED   =============
====================================================
'''
    )
    for regression in regression_list:
        print(f"{regression.benchmark}")
        print(f"Old timing: {regression.old_result}")
        print(f"New timing: {regression.new_result}")
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

other_results.sort(key=lambda x: x.benchmark)
for res in other_results:
    print(f"{res.benchmark}")
    print(f"Old timing: {res.old_result}")
    print(f"New timing: {res.new_result}")
    print("")

time_a = geomean(old_runner.complete_timings)
time_b = geomean(new_runner.complete_timings)


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
