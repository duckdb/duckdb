import os
import math
import functools
import shutil
from benchmark import BenchmarkRunner, BenchmarkRunnerConfig
from dataclasses import dataclass
from typing import Optional, List, Union, Dict
from pathlib import Path

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
parser.add_argument("--timed-runs", type=int, help="Number of timed runs per benchmark.")
parser.add_argument(
    "--clear-benchmark-cache", action="store_true", help="Clear benchmark caches prior to running", default=False
)
parser.add_argument(
    "--keep-benchmark-data", action="store_true", help="Benchmark data will not be deleted between tests", default=False
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
clear_benchmark_cache = args.clear_benchmark_cache
keep_benchmark_data = args.keep_benchmark_data
regression_threshold_seconds = args.regression_threshold_seconds
timed_runs = args.timed_runs


# how many times we will run the experiment, to be sure of the regression
NUMBER_REPETITIONS = 5
# the threshold at which we consider something a regression (percentage)
REGRESSION_THRESHOLD_PERCENTAGE = 0.1
# minimal seconds diff for something to be a regression (for very fast benchmarks)
REGRESSION_THRESHOLD_SECONDS = regression_threshold_seconds
# hide benchmark changes that are below the noise floor in the final report
DISPLAY_THRESHOLD_PERCENTAGE = 2.0

ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"
ANSI_RESET = "\033[0m"

BUCKET_UNCHANGED = "unchanged"
BUCKET_FASTER = "faster"
BUCKET_SLOWER = "slower"
BUCKET_REGRESSION = "regression"
BUCKET_FAILURE = "failure"


def in_ci():
    return os.getenv('CI') == 'true'


def suite_name():
    return Path(benchmark_file).stem


def append_step_summary(lines: List[str]):
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def format_seconds(value: Union[float, str]) -> str:
    if isinstance(value, str):
        return value
    return f"{value:.3f}s"


def format_delta_seconds(delta: float) -> str:
    if delta == 0:
        return "0.000s"
    if abs(delta) < 0.001:
        if delta < 0:
            return "-<0.001s"
        return "<0.001s"
    return f"{delta:+.3f}s"


def format_percentage(value: float) -> str:
    if math.isfinite(value):
        return f"{value:+.1f}%"
    return "+inf%"


def regression_delta(old_value: float, new_value: float) -> str:
    delta_sec = new_value - old_value
    delta_pct = ((new_value / old_value) - 1.0) * 100.0 if old_value > 0 else math.inf
    if math.isfinite(delta_pct):
        return f"{format_delta_seconds(delta_sec)} ({format_percentage(delta_pct)})"
    return format_delta_seconds(delta_sec)


def emit_github_error(title: str, message: str):
    if in_ci():
        print(f"::error title={title}::{message}")


def benchmark_timing_summary(regression: "BenchmarkResult"):
    old_timing = format_seconds(regression.old_result)
    new_timing = format_seconds(regression.new_result)
    return old_timing, new_timing


def report_regression(regression: "BenchmarkResult", summary: List[dict], summary_lines: List[str]):
    old_timing, new_timing = benchmark_timing_summary(regression)
    error_title = "Regression benchmark"
    if not isinstance(regression.old_result, str) and not isinstance(regression.new_result, str):
        delta = regression_delta(regression.old_result, regression.new_result)
        message = f"{suite_name()}: {regression.benchmark} regressed from {old_timing} to {new_timing} ({delta})"
        summary_delta = delta
    else:
        message = f"{suite_name()}: {regression.benchmark} failed while comparing base and PR benchmark runs"
        summary_delta = "benchmark run failed"
    emit_github_error(error_title, message)
    summary_lines.append(f"| `{regression.benchmark}` | `{old_timing}` | `{new_timing}` | `{summary_delta}` |")
    if regression.old_failure or regression.new_failure:
        new_data = {
            "benchmark": regression.benchmark,
            "old_failure": regression.old_failure,
            "new_failure": regression.new_failure,
        }
        summary.append(new_data)


@dataclass
class BenchmarkRow:
    result: "BenchmarkResult"
    display_name: str
    old_timing: str
    new_timing: str
    delta: str
    change: str
    bucket: str
    percentage: float = 0


def benchmark_common_prefix(benchmarks: List[str]) -> str:
    directories = [os.path.dirname(benchmark) for benchmark in benchmarks if os.path.dirname(benchmark)]
    if not directories:
        return ""
    common_prefix = os.path.commonpath(directories)
    if common_prefix in ("", "."):
        return ""
    return common_prefix + os.sep


def benchmark_display_names(benchmarks: List[str]) -> Dict[str, str]:
    common_prefix = benchmark_common_prefix(benchmarks)
    display_names = {}
    for benchmark in benchmarks:
        display_name = benchmark
        if common_prefix and benchmark.startswith(common_prefix):
            display_name = benchmark[len(common_prefix) :]
        if display_name.endswith(".benchmark"):
            display_name = display_name[: -len(".benchmark")]
        display_names[benchmark] = display_name
    return display_names


def is_regression(old_value: float, new_value: float) -> bool:
    return (old_value + REGRESSION_THRESHOLD_SECONDS) * multiply_percentage < new_value


def classify_result(result: "BenchmarkResult") -> str:
    if isinstance(result.old_result, str) or isinstance(result.new_result, str):
        return BUCKET_FAILURE
    if is_regression(result.old_result, result.new_result):
        return BUCKET_REGRESSION
    if result.new_result > result.old_result:
        return BUCKET_SLOWER
    if result.new_result < result.old_result:
        return BUCKET_FASTER
    return BUCKET_UNCHANGED


def benchmark_row(result: "BenchmarkResult", display_name: str) -> BenchmarkRow:
    if isinstance(result.old_result, str) or isinstance(result.new_result, str):
        return BenchmarkRow(
            result=result,
            display_name=display_name,
            old_timing=format_seconds(result.old_result),
            new_timing=format_seconds(result.new_result),
            delta="failed",
            change="failed",
            bucket=BUCKET_FAILURE,
        )
    delta = result.new_result - result.old_result
    percentage = ((result.new_result / result.old_result) - 1.0) * 100.0 if result.old_result > 0 else math.inf
    return BenchmarkRow(
        result=result,
        display_name=display_name,
        old_timing=format_seconds(result.old_result),
        new_timing=format_seconds(result.new_result),
        delta=format_delta_seconds(delta),
        change=format_percentage(percentage),
        bucket=classify_result(result),
        percentage=percentage,
    )


def show_in_report(row: BenchmarkRow) -> bool:
    if row.bucket in (BUCKET_REGRESSION, BUCKET_FAILURE):
        return True
    if not math.isfinite(row.percentage):
        return True
    return abs(row.percentage) >= DISPLAY_THRESHOLD_PERCENTAGE


def color_change(row: BenchmarkRow, value: str) -> str:
    if row.bucket in (BUCKET_REGRESSION, BUCKET_SLOWER, BUCKET_FAILURE):
        return f"{ANSI_RED}{value}{ANSI_RESET}"
    if row.bucket == BUCKET_FASTER:
        return f"{ANSI_GREEN}{value}{ANSI_RESET}"
    return value


def row_sort_key(row: BenchmarkRow):
    bucket_order = {
        BUCKET_UNCHANGED: 0,
        BUCKET_FASTER: 1,
        BUCKET_SLOWER: 2,
        BUCKET_REGRESSION: 3,
        BUCKET_FAILURE: 4,
    }
    if row.bucket == BUCKET_FASTER:
        bucket_value = -row.percentage
    elif row.bucket in (BUCKET_SLOWER, BUCKET_REGRESSION):
        bucket_value = row.percentage
    else:
        bucket_value = 0
    return bucket_order[row.bucket], bucket_value, row.display_name


def render_table(rows: List[BenchmarkRow]):
    if len(rows) == 0:
        return
    headers = ["benchmark", "old", "new", "delta", "change"]
    plain_rows = [[row.display_name, row.old_timing, row.new_timing, row.delta, row.change] for row in rows]
    widths = [len(header) for header in headers]
    for plain_row in plain_rows:
        for index, value in enumerate(plain_row):
            widths[index] = max(widths[index], len(value))

    header = "  ".join(headers[index].ljust(widths[index]) for index in range(len(headers)))
    separator = "  ".join("-" * widths[index] for index in range(len(headers)))
    print(header)
    print(separator)
    for row, plain_row in zip(rows, plain_rows):
        cells = []
        for index, value in enumerate(plain_row):
            padded = value.ljust(widths[index])
            if headers[index] == "change":
                padded = color_change(row, padded)
            cells.append(padded)
        print("  ".join(cells))


def print_banner(title: str):
    print("====================================================")
    print(f"==============  {title.center(26)}  =============")
    print("====================================================")
    print("")


def print_aggregate_report(
    rows: List[BenchmarkRow], common_prefix: str, result_text: str, total_count: int, hidden_noise_count: int
):
    print_banner("BENCHMARK QUERY AGGREGATES")
    print(f"suite: {suite_name()}")
    if common_prefix:
        print(f"common prefix: {common_prefix}")
    print(f"benchmarks: {total_count}")
    if timed_runs:
        print(f"timing: median of {timed_runs} timed runs")
    else:
        print("timing: median")
    print(f"threshold: +{REGRESSION_THRESHOLD_PERCENTAGE * 100.0:.1f}% and +{REGRESSION_THRESHOLD_SECONDS:.3f}s")
    print(f"display threshold: +/-{DISPLAY_THRESHOLD_PERCENTAGE:.1f}%")
    print(f"hidden noise: {hidden_noise_count} benchmarks below +/-{DISPLAY_THRESHOLD_PERCENTAGE:.1f}%")
    print(f"result: {result_text}")
    print("")
    if len(rows) == 0:
        print("0 benchmarks above display threshold")
    else:
        render_table(rows)


def print_bucket(title: str, rows: List[BenchmarkRow], hidden_noise_count: int = 0):
    print("")
    print(title)
    if title == "UNCHANGED / NOISE":
        print(f"{hidden_noise_count} benchmarks below +/-{DISPLAY_THRESHOLD_PERCENTAGE:.1f}%")
        return
    if len(rows) == 0:
        print("0 benchmarks")
        return
    render_table(rows)


def print_impact_bucket_summary(rows: List[BenchmarkRow], result_text: str, hidden_noise_count: int):
    buckets = {
        BUCKET_UNCHANGED: [row for row in rows if row.bucket == BUCKET_UNCHANGED],
        BUCKET_FASTER: [row for row in rows if row.bucket == BUCKET_FASTER],
        BUCKET_SLOWER: [row for row in rows if row.bucket == BUCKET_SLOWER],
        BUCKET_REGRESSION: [row for row in rows if row.bucket == BUCKET_REGRESSION],
        BUCKET_FAILURE: [row for row in rows if row.bucket == BUCKET_FAILURE],
    }
    print("")
    print_banner("IMPACT BUCKETS SUMMARY")
    print(f"result: {result_text}")
    print_bucket("UNCHANGED / NOISE", buckets[BUCKET_UNCHANGED], hidden_noise_count)
    print_bucket("FASTER", buckets[BUCKET_FASTER])
    print_bucket("SLOWER BELOW THRESHOLD", buckets[BUCKET_SLOWER])
    print_bucket("REGRESSIONS", buckets[BUCKET_REGRESSION])
    if len(buckets[BUCKET_FAILURE]) > 0:
        print_bucket("FAILURES", buckets[BUCKET_FAILURE])


def print_geomean_summary(time_a: Union[float, str], time_b: Union[float, str]):
    print("")
    if isinstance(time_a, str) or isinstance(time_b, str):
        print(f"geomean old: {time_a}")
        print(f"geomean new: {time_b}")
        return
    delta_pct = ((time_b / time_a) - 1.0) * 100.0 if time_a > 0 else math.inf
    row = BenchmarkRow(
        result=BenchmarkResult("geomean", time_a, time_b),
        display_name="geomean",
        old_timing=format_seconds(time_a),
        new_timing=format_seconds(time_b),
        delta=format_delta_seconds(time_b - time_a),
        change=format_percentage(delta_pct),
        bucket=BUCKET_SLOWER if delta_pct > 0 else BUCKET_FASTER if delta_pct < 0 else BUCKET_UNCHANGED,
        percentage=delta_pct,
    )
    print(f"geomean: {format_seconds(time_a)} -> {format_seconds(time_b)}  {color_change(row, row.change)}")


if not os.path.isfile(old_runner_path):
    print(f"Failed to find old runner {old_runner_path}")
    exit(1)

if not os.path.isfile(new_runner_path):
    print(f"Failed to find new runner {new_runner_path}")
    exit(1)

if clear_benchmark_cache:
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
old_runner = BenchmarkRunner(
    BenchmarkRunnerConfig.from_params(old_runner_path, benchmark_file, runner_label="old", **config_dict)
)
new_runner = BenchmarkRunner(
    BenchmarkRunnerConfig.from_params(new_runner_path, benchmark_file, runner_label="new", **config_dict)
)

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

final_regression_results = regression_list + error_list
all_results = other_results + final_regression_results
display_names = benchmark_display_names([result.benchmark for result in all_results])
rows = [benchmark_row(result, display_names[result.benchmark]) for result in all_results]
rows.sort(key=row_sort_key)
visible_rows = [row for row in rows if show_in_report(row)]
hidden_noise_count = len(rows) - len(visible_rows)

exit_code = 0
summary = []
if len(final_regression_results) > 0:
    exit_code = 1
    summary_lines = [
        f"## Regression Suite: `{suite_name()}`",
        "",
        "| Benchmark | Base | PR | Delta |",
        "| --- | --- | --- | --- |",
    ]
    for regression in final_regression_results:
        report_regression(regression, summary, summary_lines)
    append_step_summary(summary_lines + [""])

has_failures = any(row.bucket == BUCKET_FAILURE for row in rows)
has_regressions = any(row.bucket == BUCKET_REGRESSION for row in rows)
if has_failures:
    result_text = "benchmark failure detected"
elif has_regressions:
    result_text = "regression detected"
else:
    result_text = "no regressions detected"

common_prefix = benchmark_common_prefix([result.benchmark for result in all_results])
print_aggregate_report(visible_rows, common_prefix, result_text, len(rows), hidden_noise_count)

time_a = geomean(old_runner.complete_timings)
time_b = geomean(new_runner.complete_timings)
print_geomean_summary(time_a, time_b)
print_impact_bucket_summary(visible_rows, result_text, hidden_noise_count)

# nuke cached benchmark data between runs
if not keep_benchmark_data:
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
    prefix = "::error::" if in_ci() else ""
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
