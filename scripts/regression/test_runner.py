import argparse
import functools
import math
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from benchmark import BenchmarkRunner
from comparison import (
    CONFIRMATION_TARGET_SECONDS,
    MAX_CONFIRMATION_RUNS,
    MIN_CONFIRMATION_RUNS,
    BenchmarkMeasurement,
    benchmark_measurement,
    confirmation_run_count,
)


print = functools.partial(print, flush=True)

INITIAL_BATCH_SIZE = 5
INITIAL_RUNS = 2 * INITIAL_BATCH_SIZE
REGRESSION_LIMIT = 1.10
NOISE_THRESHOLD = 0.02
CONFIDENCE_PERCENTAGE = 95

ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"
ANSI_YELLOW = "\033[33m"
ANSI_RESET = "\033[0m"

BUCKET_UNCHANGED = "unchanged"
BUCKET_FASTER = "faster"
BUCKET_SLOWER = "slower"
BUCKET_REGRESSION = "regression"
BUCKET_UNCONFIRMED = "unconfirmed"
BUCKET_FAILURE = "failure"

OUTCOME_NO_REGRESSION = "no_regression"
OUTCOME_CONFIRMED_REGRESSION = "confirmed_regression"
OUTCOME_UNCONFIRMED_REGRESSION = "unconfirmed_regression"
OUTCOME_FAILURE = "failure"


@dataclass
class BenchmarkResult:
    benchmark: str
    initial_measurement: Optional[BenchmarkMeasurement] = None
    measurement: Optional[BenchmarkMeasurement] = None
    old_failure: Optional[str] = None
    new_failure: Optional[str] = None
    initial_runs: int = 0
    confirmation_runs: int = 0
    outcome: str = OUTCOME_NO_REGRESSION

    @property
    def runs(self) -> int:
        return self.initial_runs + self.confirmation_runs


@dataclass
class BenchmarkRow:
    result: Optional[BenchmarkResult]
    display_name: str
    old_timing: str
    new_timing: str
    delta: str
    change: str
    runs: str
    bucket: str
    percentage: float = 0


def parse_arguments():
    parser = argparse.ArgumentParser(description="Compare benchmarks from base and PR benchmark runners.")
    parser.add_argument("--old", required=True, help="Path to the base benchmark runner.")
    parser.add_argument("--new", required=True, help="Path to the PR benchmark runner.")
    parser.add_argument("--benchmarks", required=True, help="Path to the benchmark list.")
    parser.add_argument("--threads", type=int, help="Number of threads used by each benchmark runner.")
    parser.add_argument("--memory-limit", help="Memory limit used by each benchmark runner.")
    parser.add_argument("--verbose", action="store_true", help="Print raw benchmark runner output.")
    parser.add_argument("--nofail", action="store_true", help="Report confirmed regressions without failing.")
    parser.add_argument("--disable-timeout", action="store_true", help="Disable the benchmark runner timeout.")
    parser.add_argument(
        "--benchmark-cache",
        choices=("keep", "clear"),
        default="keep",
        help="Keep benchmark data or clear runner caches before running (default: keep).",
    )
    return parser.parse_args()


def clear_benchmark_caches(runner_paths: List[str]):
    cache_paths = {
        os.path.abspath(os.path.join(os.path.dirname(runner_path), "..", "..", "..", "duckdb_benchmark_data"))
        for runner_path in runner_paths
    }
    for cache_path in cache_paths:
        shutil.rmtree(cache_path, ignore_errors=True)


def run_paired_samples(
    old_runner: BenchmarkRunner,
    new_runner: BenchmarkRunner,
    benchmark: str,
    requested_runs: int,
    initial_batch_index: int,
):
    old_timings = []
    new_timings = []
    batch_index = initial_batch_index
    batch_sizes = [(requested_runs + 1) // 2, requested_runs // 2]

    for batch_size in batch_sizes:
        if batch_index % 2 == 0:
            old_batch, old_failure = old_runner.run(benchmark, batch_size)
            new_batch, new_failure = new_runner.run(benchmark, batch_size)
        else:
            new_batch, new_failure = new_runner.run(benchmark, batch_size)
            old_batch, old_failure = old_runner.run(benchmark, batch_size)

        old_batch = old_batch or []
        new_batch = new_batch or []
        if old_failure or new_failure:
            return old_timings, new_timings, old_failure, new_failure, batch_index
        if len(old_batch) != len(new_batch):
            failure = f"Paired benchmark batches produced different run counts: {len(old_batch)} and {len(new_batch)}"
            return old_timings, new_timings, failure, failure, batch_index

        old_timings.extend(old_batch)
        new_timings.extend(new_batch)
        batch_index += 1

    return old_timings, new_timings, None, None, batch_index


def failed_benchmark_result(
    benchmark: str,
    old_failure: Optional[str],
    new_failure: Optional[str],
    initial_measurement: Optional[BenchmarkMeasurement],
    initial_runs: int,
    confirmation_runs: int,
) -> BenchmarkResult:
    return BenchmarkResult(
        benchmark=benchmark,
        initial_measurement=initial_measurement,
        old_failure=old_failure,
        new_failure=new_failure,
        initial_runs=initial_runs,
        confirmation_runs=confirmation_runs,
        outcome=OUTCOME_FAILURE,
    )


def run_paired_benchmark(
    old_runner: BenchmarkRunner, new_runner: BenchmarkRunner, benchmark: str, verbose: bool
) -> BenchmarkResult:
    old_initial, new_initial, old_failure, new_failure, batch_index = run_paired_samples(
        old_runner, new_runner, benchmark, INITIAL_RUNS, 0
    )
    initial_count = min(len(old_initial), len(new_initial))
    if old_failure or new_failure:
        return failed_benchmark_result(benchmark, old_failure, new_failure, None, initial_count, 0)

    initial_measurement = benchmark_measurement(old_initial, new_initial)
    is_candidate = (
        initial_measurement.ratio < 1.0 - NOISE_THRESHOLD or initial_measurement.ratio > 1.0 + NOISE_THRESHOLD
    )
    if verbose:
        decision = "additional sampling required" if is_candidate else "within noise threshold"
        print(
            f"initial sampling: {benchmark}: {initial_count} paired runs, "
            f"median PR/base {initial_measurement.ratio:.3f}x ({decision})"
        )

    if not is_candidate:
        return BenchmarkResult(
            benchmark=benchmark,
            initial_measurement=initial_measurement,
            measurement=initial_measurement,
            initial_runs=initial_count,
        )

    requested_confirmation_runs = confirmation_run_count(initial_measurement)
    old_confirmation, new_confirmation, old_failure, new_failure, _ = run_paired_samples(
        old_runner,
        new_runner,
        benchmark,
        requested_confirmation_runs,
        batch_index,
    )
    confirmation_count = min(len(old_confirmation), len(new_confirmation))
    if old_failure or new_failure:
        return failed_benchmark_result(
            benchmark,
            old_failure,
            new_failure,
            initial_measurement,
            initial_count,
            confirmation_count,
        )

    confirmation_measurement = benchmark_measurement(old_confirmation, new_confirmation)
    lower_bound, upper_bound = confirmation_measurement.ratio_interval
    if confirmation_measurement.ratio > REGRESSION_LIMIT:
        if lower_bound > REGRESSION_LIMIT:
            outcome = OUTCOME_CONFIRMED_REGRESSION
            decision = "confirmed regression"
        else:
            outcome = OUTCOME_UNCONFIRMED_REGRESSION
            decision = "regression inconclusive"
    elif confirmation_measurement.ratio > 1.0 + NOISE_THRESHOLD:
        outcome = OUTCOME_NO_REGRESSION
        decision = "slower below regression limit"
    elif confirmation_measurement.ratio < 1.0 - NOISE_THRESHOLD:
        outcome = OUTCOME_NO_REGRESSION
        decision = "faster"
    else:
        outcome = OUTCOME_NO_REGRESSION
        decision = "within noise threshold"

    if verbose:
        print(
            f"confirmation sampling: {benchmark}: {confirmation_count} paired runs, "
            f"median PR/base {confirmation_measurement.ratio:.3f}x; "
            f"{CONFIDENCE_PERCENTAGE}% CI for PR/base median ratio: {lower_bound:.3f}x to {upper_bound:.3f}x; "
            f"{decision}"
        )

    return BenchmarkResult(
        benchmark=benchmark,
        initial_measurement=initial_measurement,
        measurement=confirmation_measurement,
        initial_runs=initial_count,
        confirmation_runs=confirmation_count,
        outcome=outcome,
    )


def in_ci() -> bool:
    return os.getenv("CI") == "true"


def append_step_summary(lines: List[str]):
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a", encoding="utf-8") as summary_file:
        summary_file.write("\n".join(lines))
        summary_file.write("\n")


def format_seconds(value: float) -> str:
    return f"{value:.3f}s"


def format_delta_seconds(delta: float) -> str:
    if delta == 0:
        return "0.000s"
    if abs(delta) < 0.001:
        return "-<0.001s" if delta < 0 else "<0.001s"
    return f"{delta:+.3f}s"


def format_percentage(value: float) -> str:
    return f"{value:+.1f}%" if math.isfinite(value) else "+inf%"


def regression_delta(measurement: BenchmarkMeasurement) -> str:
    delta_seconds = measurement.new_timing - measurement.old_timing
    delta_percentage = ((measurement.new_timing / measurement.old_timing) - 1.0) * 100.0
    return f"{format_delta_seconds(delta_seconds)} ({format_percentage(delta_percentage)})"


def confidence_text(result: BenchmarkResult) -> str:
    if result.measurement is None:
        return "unavailable"
    lower_bound, upper_bound = result.measurement.ratio_interval
    return f"{lower_bound:.3f}x to {upper_bound:.3f}x"


def emit_github_error(title: str, message: str):
    if in_ci():
        print(f"::error title={title}::{message}")


def emit_github_warning(title: str, message: str):
    if in_ci():
        print(f"::warning title={title}::{message}")


def report_regression(
    result: BenchmarkResult, suite: str, failure_summary: List[BenchmarkResult], summary_lines: List[str]
):
    if result.outcome == OUTCOME_FAILURE:
        message = f"{suite}: {result.benchmark} failed while comparing base and PR benchmark runs"
        old_timing = "failed"
        new_timing = "failed"
        summary_delta = "benchmark run failed"
        failure_summary.append(result)
    else:
        measurement = result.measurement
        assert measurement is not None
        old_timing = format_seconds(measurement.old_timing)
        new_timing = format_seconds(measurement.new_timing)
        summary_delta = regression_delta(measurement)
        message = (
            f"{suite}: {result.benchmark} regressed from {old_timing} to {new_timing} ({summary_delta}); "
            f"{CONFIDENCE_PERCENTAGE}% CI for PR/base median ratio {confidence_text(result)}, "
            f"regression limit {REGRESSION_LIMIT:.3f}x"
        )
    emit_github_error("Regression benchmark", message)
    summary_lines.append(
        f"| `{result.benchmark}` | `{old_timing}` | `{new_timing}` | `{summary_delta}` | `{format_run_count(result)}` |"
    )


def report_unconfirmed(result: BenchmarkResult, suite: str, summary_lines: List[str]):
    measurement = result.measurement
    assert measurement is not None
    old_timing = format_seconds(measurement.old_timing)
    new_timing = format_seconds(measurement.new_timing)
    delta = regression_delta(measurement)
    confidence = confidence_text(result)
    message = (
        f"{suite}: {result.benchmark} has a confirmation median above the regression limit but remains "
        f"inconclusive ({delta}); {CONFIDENCE_PERCENTAGE}% CI for PR/base median ratio {confidence}, "
        f"regression limit {REGRESSION_LIMIT:.3f}x"
    )
    emit_github_warning("Inconclusive regression benchmark", message)
    summary_lines.append(
        f"| `{result.benchmark}` | `{old_timing}` | `{new_timing}` | `{delta}` | "
        f"`{format_run_count(result)}` | `{confidence}` |"
    )


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
        display_name = (
            benchmark[len(common_prefix) :] if common_prefix and benchmark.startswith(common_prefix) else benchmark
        )
        if display_name.endswith(".benchmark"):
            display_name = display_name[: -len(".benchmark")]
        display_names[benchmark] = display_name
    return display_names


def classify_result(result: BenchmarkResult) -> str:
    if result.outcome == OUTCOME_FAILURE:
        return BUCKET_FAILURE
    if result.outcome == OUTCOME_CONFIRMED_REGRESSION:
        return BUCKET_REGRESSION
    if result.outcome == OUTCOME_UNCONFIRMED_REGRESSION:
        return BUCKET_UNCONFIRMED

    assert result.measurement is not None
    ratio = result.measurement.ratio
    if ratio > 1.0 + NOISE_THRESHOLD:
        return BUCKET_SLOWER
    if ratio < 1.0 - NOISE_THRESHOLD:
        return BUCKET_FASTER
    return BUCKET_UNCHANGED


def format_run_count(result: BenchmarkResult) -> str:
    if result.confirmation_runs:
        return f"{result.initial_runs}+{result.confirmation_runs}"
    return str(result.initial_runs)


def benchmark_row(result: BenchmarkResult, display_name: str) -> BenchmarkRow:
    bucket = classify_result(result)
    if result.outcome == OUTCOME_FAILURE:
        return BenchmarkRow(
            result, display_name, "failed", "failed", "failed", "failed", format_run_count(result), bucket
        )

    measurement = result.measurement
    assert measurement is not None
    delta = measurement.new_timing - measurement.old_timing
    percentage = ((measurement.new_timing / measurement.old_timing) - 1.0) * 100.0
    return BenchmarkRow(
        result=result,
        display_name=display_name,
        old_timing=format_seconds(measurement.old_timing),
        new_timing=format_seconds(measurement.new_timing),
        delta=format_delta_seconds(delta),
        change=format_percentage(percentage),
        runs=format_run_count(result),
        bucket=bucket,
        percentage=percentage,
    )


def color_change(bucket: str, value: str) -> str:
    if bucket in (BUCKET_REGRESSION, BUCKET_SLOWER, BUCKET_FAILURE):
        return f"{ANSI_RED}{value}{ANSI_RESET}"
    if bucket == BUCKET_UNCONFIRMED:
        return f"{ANSI_YELLOW}{value}{ANSI_RESET}"
    if bucket == BUCKET_FASTER:
        return f"{ANSI_GREEN}{value}{ANSI_RESET}"
    return value


def row_sort_key(row: BenchmarkRow):
    bucket_order = {
        BUCKET_UNCHANGED: 0,
        BUCKET_FASTER: 1,
        BUCKET_SLOWER: 2,
        BUCKET_UNCONFIRMED: 3,
        BUCKET_REGRESSION: 4,
        BUCKET_FAILURE: 5,
    }
    if row.bucket == BUCKET_FASTER:
        bucket_value = -row.percentage
    elif row.bucket in (BUCKET_SLOWER, BUCKET_UNCONFIRMED, BUCKET_REGRESSION):
        bucket_value = row.percentage
    else:
        bucket_value = 0
    return bucket_order[row.bucket], bucket_value, row.display_name


def render_table(rows: List[BenchmarkRow]):
    if not rows:
        return
    headers = ["benchmark", "base", "PR", "delta", "change", "runs"]
    plain_rows = [[row.display_name, row.old_timing, row.new_timing, row.delta, row.change, row.runs] for row in rows]
    widths = [len(header) for header in headers]
    for plain_row in plain_rows:
        for index, value in enumerate(plain_row):
            widths[index] = max(widths[index], len(value))

    print("  ".join(headers[index].ljust(widths[index]) for index in range(len(headers))))
    print("  ".join("-" * widths[index] for index in range(len(headers))))
    for row, plain_row in zip(rows, plain_rows):
        cells = []
        for index, value in enumerate(plain_row):
            padded = value.ljust(widths[index])
            if headers[index] == "change":
                padded = color_change(row.bucket, padded)
            cells.append(padded)
        print("  ".join(cells))


def print_bucket(title: str, rows: List[BenchmarkRow], unchanged_count: int = 0):
    print("")
    print(title)
    if title == "UNCHANGED / NOISE":
        print(f"{unchanged_count} benchmarks whose median change is within +/-2%")
    elif rows:
        render_table(rows)
    else:
        print("0 benchmarks")


def geomean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return math.exp(math.fsum(math.log(value) for value in values) / len(values))


def print_geomean_summary(old_geomean: Optional[float], new_geomean: Optional[float]):
    print("")
    if old_geomean is None or new_geomean is None:
        print(f"geomean (initial {INITIAL_RUNS} samples): unavailable")
        return
    percentage = ((new_geomean / old_geomean) - 1.0) * 100.0
    if percentage > NOISE_THRESHOLD * 100.0:
        bucket = BUCKET_SLOWER
    elif percentage < -NOISE_THRESHOLD * 100.0:
        bucket = BUCKET_FASTER
    else:
        bucket = BUCKET_UNCHANGED
    change = color_change(bucket, format_percentage(percentage))
    print(
        f"geomean (initial {INITIAL_RUNS} samples): "
        f"{format_seconds(old_geomean)} -> {format_seconds(new_geomean)}  {change}"
    )


def print_benchmark_report(
    rows: List[BenchmarkRow],
    common_prefix: str,
    result_text: str,
    old_geomean: Optional[float],
    new_geomean: Optional[float],
):
    buckets = {
        bucket: [row for row in rows if row.bucket == bucket]
        for bucket in (
            BUCKET_UNCHANGED,
            BUCKET_FASTER,
            BUCKET_SLOWER,
            BUCKET_UNCONFIRMED,
            BUCKET_REGRESSION,
            BUCKET_FAILURE,
        )
    }

    print("====================================================")
    print("==============  BENCHMARK QUERY RESULTS  ===========")
    print("====================================================")
    print("")
    if common_prefix:
        print(f"common prefix: {common_prefix}")
    print(f"benchmarks: {len(rows)}")
    print(f"initial sampling: {INITIAL_RUNS} runs per binary in 2 batches of {INITIAL_BATCH_SIZE}")
    print(
        f"confirmation sampling: ceil({CONFIRMATION_TARGET_SECONDS:g}s / faster-side median), "
        f"clamped to {MIN_CONFIRMATION_RUNS}-{MAX_CONFIRMATION_RUNS} runs per binary"
    )
    print(f"confirmation candidates: initial median outside +/-{NOISE_THRESHOLD * 100:.0f}%")
    print(f"regression limit: {REGRESSION_LIMIT:.3f}x (+{(REGRESSION_LIMIT - 1.0) * 100:.0f}%)")
    print(
        f"reporting: confirmation median with +/-{NOISE_THRESHOLD * 100:.0f}% noise threshold; "
        f"regressions require {CONFIDENCE_PERCENTAGE}% confidence"
    )
    print(f"result: {result_text}")
    print_geomean_summary(old_geomean, new_geomean)
    print_bucket("UNCHANGED / NOISE", buckets[BUCKET_UNCHANGED], len(buckets[BUCKET_UNCHANGED]))
    print_bucket("FASTER", buckets[BUCKET_FASTER])
    print_bucket("SLOWER BELOW REGRESSION LIMIT", buckets[BUCKET_SLOWER])
    print_bucket("INCONCLUSIVE REGRESSION CANDIDATES", buckets[BUCKET_UNCONFIRMED])
    print_bucket("REGRESSIONS", buckets[BUCKET_REGRESSION])
    if buckets[BUCKET_FAILURE]:
        print_bucket("FAILURES", buckets[BUCKET_FAILURE])


def print_failure_summary(failures: List[BenchmarkResult]):
    if not failures:
        return
    print(
        """\n
====================================================
================  FAILURES SUMMARY  ================
====================================================
"""
    )
    for index, result in enumerate(failures, start=1):
        prefix = "::error::" if in_ci() else ""
        print(f"{prefix}{index}: {result.benchmark}")
        if result.old_failure != result.new_failure:
            print("Base:\n", result.old_failure)
            print("PR:\n", result.new_failure)
        else:
            print(result.old_failure)
        print("-", 52)


def main() -> int:
    args = parse_arguments()
    for label, path in (("base", args.old), ("PR", args.new)):
        if not os.path.isfile(path):
            print(f"Failed to find {label} runner {path}")
            return 1
    if not os.path.isfile(args.benchmarks):
        print(f"Failed to find benchmark list {args.benchmarks}")
        return 1

    if args.benchmark_cache == "clear":
        clear_benchmark_caches([args.old, args.new])

    with open(args.benchmarks, "r", encoding="utf-8") as benchmark_file:
        benchmarks = [line.strip() for line in benchmark_file if line.strip()]
    suite = Path(args.benchmarks).stem
    old_runner = BenchmarkRunner(args.old, "base", args.threads, args.memory_limit, args.verbose, args.disable_timeout)
    new_runner = BenchmarkRunner(args.new, "PR", args.threads, args.memory_limit, args.verbose, args.disable_timeout)
    results = [run_paired_benchmark(old_runner, new_runner, benchmark, args.verbose) for benchmark in benchmarks]

    failing_results = [
        result
        for result in results
        if result.outcome == OUTCOME_FAILURE or (result.outcome == OUTCOME_CONFIRMED_REGRESSION and not args.nofail)
    ]
    inconclusive_results = [result for result in results if result.outcome == OUTCOME_UNCONFIRMED_REGRESSION]
    failure_summary = []
    if failing_results:
        summary_lines = [
            f"## Regression Suite: `{suite}`",
            "",
            "| Benchmark | Base | PR | Delta | Runs |",
            "| --- | --- | --- | --- | --- |",
        ]
        for result in failing_results:
            report_regression(result, suite, failure_summary, summary_lines)
        append_step_summary(summary_lines + [""])

    if inconclusive_results:
        summary_lines = [
            f"## Inconclusive Regression Candidates: `{suite}`",
            "",
            "| Benchmark | Base | PR | Delta | Runs | 95% PR/base median ratio interval |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for result in inconclusive_results:
            report_unconfirmed(result, suite, summary_lines)
        append_step_summary(summary_lines + [""])

    display_names = benchmark_display_names([result.benchmark for result in results])
    rows = [benchmark_row(result, display_names[result.benchmark]) for result in results]
    rows.sort(key=row_sort_key)
    if any(result.outcome == OUTCOME_FAILURE for result in results):
        result_text = "benchmark failure detected"
    elif any(result.outcome == OUTCOME_CONFIRMED_REGRESSION for result in results):
        result_text = "regression detected"
    elif inconclusive_results:
        result_text = "no confirmed regressions; inconclusive candidates reported"
    else:
        result_text = "no regressions detected"

    initial_measurements = [result.initial_measurement for result in results if result.initial_measurement]
    old_geomean = geomean([measurement.old_timing for measurement in initial_measurements])
    new_geomean = geomean([measurement.new_timing for measurement in initial_measurements])
    print_benchmark_report(
        rows,
        benchmark_common_prefix([result.benchmark for result in results]),
        result_text,
        old_geomean,
        new_geomean,
    )
    print_failure_summary(failure_summary)
    return 1 if failing_results else 0


if __name__ == "__main__":
    raise SystemExit(main())
