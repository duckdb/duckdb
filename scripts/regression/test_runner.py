import os
import sys
import math
import functools
from benchmark import BenchmarkRunner, BenchmarkRunnerConfig
from dataclasses import dataclass
from typing import Optional, List, Union

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
parser.add_argument("--nofail", action="store_true", help="Do not fail on regression.")
parser.add_argument("--disable-timeout", action="store_true", help="Disable timeout.")
parser.add_argument("--max-timeout", type=int, default=3600, help="Set maximum timeout in seconds (default: 3600).")
parser.add_argument("--root-dir", type=str, default="", help="Root directory.")
parser.add_argument("--no-summary", action="store_true", help="No summary in the end.")
parser.add_argument(
    "--regression-threshold-seconds",
    type=float,
    default=0.05,
    help="The threshold at which we consider individual benchmark as a regression (seconds)",
)
parser.add_argument(
    "--max-allowed-regression-percentage",
    type=int,
    default=1,
    help="Max regression percentage for overall test suite to be a regression",
)

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
no_summary = args.no_summary
regression_threshold_seconds = args.regression_threshold_seconds
max_allowed_regression_percentage = args.max_allowed_regression_percentage

# how many times we will run the experiment, to be sure of the regression
NUMBER_REPETITIONS = 5
# the threshold at which we consider individual benchmark a regression (percentage)
REGRESSION_THRESHOLD_PERCENTAGE = 0.1
# minimal seconds diff for something to be a regression (for very fast benchmarks)
REGRESSION_THRESHOLD_SECONDS = regression_threshold_seconds
# max regression percentage for overall test suite to be a regression
MAX_ALLOWED_REGRESS_PERCENTAGE = max_allowed_regression_percentage

if not os.path.isfile(old_runner_path):
    print(f"Failed to find old runner {old_runner_path}")
    sys.exit(1)

if not os.path.isfile(new_runner_path):
    print(f"Failed to find new runner {new_runner_path}")
    sys.exit(1)

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
    improvement: Optional[int] = None


multiply_percentage = 1.0 + REGRESSION_THRESHOLD_PERCENTAGE
other_results: List[BenchmarkResult] = []
error_list: List[BenchmarkResult] = []
improvements_list: List[BenchmarkResult] = []
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
            try:
                improved_by = int((float(old_res) - float(new_res)) * 100.0 / float(old_res))
            except Exception:
                improved_by = 0
            if improved_by > 0:
                improvements_list.append(BenchmarkResult(benchmark, old_res, new_res, improvement=improved_by))
            else:
                other_results.append(BenchmarkResult(benchmark, old_res, new_res))
    benchmark_list = [res.benchmark for res in regression_list]

time_old = geomean(old_runner.complete_timings)
time_new = geomean(new_runner.complete_timings)

exit_code = 0
regression_list.extend(error_list)
summary = []
if len(regression_list) > 0:
    if not (isinstance(time_old, str) or isinstance(time_new, str)):
        regression_percentage = int((time_new - time_old) * 100.0 / time_old)
    else:
        regression_percentage = 0
    if (
        isinstance(MAX_ALLOWED_REGRESS_PERCENTAGE, int)
        and isinstance(regression_percentage, int)
        and regression_percentage < MAX_ALLOWED_REGRESS_PERCENTAGE
    ):
        # allow individual regressions less than 10% when overall geomean had improved or hadn't change (on large benchmarks)
        regressions_header = 'ALLOWED REGRESSIONS'
    else:
        regressions_header = 'REGRESSIONS DETECTED'
        exit_code = 1

    if len(regression_list) > 0:
        print(
            f'''====================================================
==============  {regressions_header}   ==============
====================================================
'''
        )

        for regression in regression_list:
            print("")
            print(f"{regression.benchmark}")
            print(f"Old timing: {regression.old_result}")
            print(f"New timing: {regression.new_result}")
            # add to the FAILURES SUMMARY
            if regression.old_failure or regression.new_failure:
                new_data = {
                    "benchmark": regression.benchmark,
                    "old_failure": regression.old_failure,
                    "new_failure": regression.new_failure,
                }
                summary.append(new_data)
            print("")

        if not (isinstance(time_old, str) or isinstance(time_new, str)):
            delta_pct = int(((time_old - time_new) * 100) / time_old)
            if delta_pct > 0:
                print(f"Old timing geometric mean: {time_old}, roughly {regression_percentage}% faster")
            else:
                print(f"Old timing geometric mean: {time_old}, roughly {abs(regression_percentage)})% faster")

            print(f"New timing geometric mean: {time_new}")
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

if len(improvements_list) > 0:
    print(
        '''====================================================
============== IMPROVEMENTS DETECTED  ==============
====================================================
'''
    )
improvements_list.sort(key=lambda x: x.benchmark)
for res in improvements_list:
    print(f"{res.benchmark}")
    print(f"Old timing: {res.old_result}")
    print(f"New timing: {res.new_result}")
    print(f"Improved by: {res.improvement}%")
    print("")


print("")
if isinstance(time_old, str) or isinstance(time_new, str):
    print(f"Old: {time_old}")
    print(f"New: {time_new}")
else:
    threshold = 1 + MAX_ALLOWED_REGRESS_PERCENTAGE / 100
    if time_new < time_old / threshold:
        pct = int(((time_old - time_new) * 100.0) / time_old)
        print(f"Old timing geometric mean: {time_old}")
        print(f"New timing geometric mean: {time_new}, roughly {pct}% faster")
    elif time_new > time_old * threshold:
        pct = int(((time_old - time_new) * 100.0) / time_old)
        print(f"Old timing geometric mean: {time_old}, roughly {pct}% faster")
        print(f"New timing geometric mean: {time_new}")
    else:
        print(f"Old timing geometric mean: {time_old}")
        print(f"New timing geometric mean: {time_new}")

if summary and not no_summary:
    print(
        '''\n\n====================================================
================  FAILURES SUMMARY  ================
====================================================
'''
    )
    for i, failure_message in enumerate(summary, start=1):
        print(f"{i}: ", failure_message["benchmark"])
        if failure_message["old_failure"] != failure_message["new_failure"]:
            print("Old:\n", failure_message["old_failure"])
            print("New:\n", failure_message["new_failure"])
        else:
            print(failure_message["old_failure"])
        print("-", 52)

sys.exit(exit_code)
