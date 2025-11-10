import os
import math
import functools
import shutil
from benchmark import BenchmarkRunner, BenchmarkRunnerConfig
from dataclasses import dataclass
from typing import Optional, List, Union
import subprocess

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


import argparse

# Set up the argument parser
parser = argparse.ArgumentParser(description="Benchmark script with old and new runners.")

# Define the arguments
parser.add_argument("--old", type=str, help="Path to the old runner.", required=True)
parser.add_argument("--new", type=str, help="Path to the new runner.", required=True)
parser.add_argument("--benchmarks", type=str, help="Path to the benchmark file.", required=True)
parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
parser.add_argument("--threads", type=int, help="Number of threads to use.")
parser.add_argument("--memory_limit", type=str, help="Memory limit to use.")
parser.add_argument("--nofail", action="store_true", help="Do not fail on regression.")
parser.add_argument("--disable-timeout", action="store_true", help="Disable timeout.")
parser.add_argument("--max-timeout", type=int, default=3600, help="Set maximum timeout in seconds (default: 3600).")
parser.add_argument("--root-dir", type=str, default="", help="Root directory.")
parser.add_argument("--no-summary", type=str, default=False, help="No summary in the end.")
parser.add_argument(
    "--clear-benchmark-cache", action="store_true", help="Clear benchmark caches prior to running", default=False
)
parser.add_argument(
    "--regression-threshold-seconds",
    type=float,
    default=0.05,
    help="REGRESSION_THRESHOLD_SECONDS value for large benchmarks.",
)

# Parse the arguments
args = parser.parse_args()

# Assign parsed arguments to variables
old_runner_path = args.old
new_runner_path = args.new
benchmark_file = args.benchmarks
verbose = args.verbose
threads = args.threads
memory_limit = args.memory_limit
no_regression_fail = args.nofail
disable_timeout = args.disable_timeout
max_timeout = args.max_timeout
root_dir = args.root_dir
no_summary = args.no_summary
regression_threshold_seconds = args.regression_threshold_seconds


# how many times we will run the experiment, to be sure of the regression
NUMBER_REPETITIONS = 5
# the threshold at which we consider something a regression (percentage)
REGRESSION_THRESHOLD_PERCENTAGE = 0.1
# minimal seconds diff for something to be a regression (for very fast benchmarks)
REGRESSION_THRESHOLD_SECONDS = regression_threshold_seconds

if not os.path.isfile(old_runner_path):
    print(f"Failed to find old runner {old_runner_path}")
    exit(1)

if not os.path.isfile(new_runner_path):
    print(f"Failed to find new runner {new_runner_path}")
    exit(1)

if args.clear_benchmark_cache:
    old_cache_path = os.path.join(os.path.dirname(old_runner_path), '..', '..', '..', 'duckdb_benchmark_data')
    new_cache_path = os.path.join(os.path.dirname(new_runner_path), '..', '..', '..', 'duckdb_benchmark_data')
    try:
        shutil.rmtree(old_cache_path)
    except:
        pass
    try:
        shutil.rmtree(new_cache_path)
    except:
        pass

config_dict = vars(args)
old_runner = BenchmarkRunner(BenchmarkRunnerConfig.from_params(old_runner_path, benchmark_file, **config_dict))
new_runner = BenchmarkRunner(BenchmarkRunnerConfig.from_params(new_runner_path, benchmark_file, **config_dict))

benchmark_list = old_runner.benchmark_list

summary = []


@dataclass
class BenchmarkResult:
    benchmark: str
    old_result: Union[float, str]
    new_result: Union[float, str]
    old_failure: Optional[str] = None
    new_failure: Optional[str] = None


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

    old_results, old_failures = old_runner.run_benchmarks(benchmark_list)
    new_results, new_failures = new_runner.run_benchmarks(benchmark_list)

    for benchmark in benchmark_list:
        old_res = old_results[benchmark]
        new_res = new_results[benchmark]

        old_fail = old_failures[benchmark]
        new_fail = new_failures[benchmark]

        if isinstance(old_res, str) or isinstance(new_res, str):
            # benchmark failed to run - always a regression
            error_list.append(BenchmarkResult(benchmark, old_res, new_res, old_fail, new_fail))
        elif (no_regression_fail == False) and (
            (old_res + REGRESSION_THRESHOLD_SECONDS) * multiply_percentage < new_res
        ):
            regression_list.append(BenchmarkResult(benchmark, old_res, new_res))
        else:
            other_results.append(BenchmarkResult(benchmark, old_res, new_res))
    benchmark_list = [res.benchmark for res in regression_list]

exit_code = 0
regression_list.extend(error_list)
summary = []
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
        if regression.old_failure or regression.new_failure:
            new_data = {
                "benchmark": regression.benchmark,
                "old_failure": regression.old_failure,
                "new_failure": regression.new_failure,
            }
            summary.append(new_data)
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

if summary and not no_summary:
    print(
        '''\n\n====================================================
================  FAILURES SUMMARY  ================
====================================================
'''
    )
    # check the value is "true" otherwise you'll see the prefix in local run outputs
    prefix = "::error::" if ('CI' in os.environ and os.getenv('CI') == 'true') else ""
    for i, failure_message in enumerate(summary, start=1):
        prefix_str = f"{prefix}{i}" if len(prefix) > 0 else f"{i}"
        print(f"{prefix_str}: ", failure_message["benchmark"])
        if failure_message["old_failure"] != failure_message["new_failure"]:
            print("Old:\n", failure_message["old_failure"])
            print("New:\n", failure_message["new_failure"])
        else:
            print(failure_message["old_failure"])
        print("-", 52)

exit(exit_code)
