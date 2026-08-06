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
    MAX_CONFIRMATION_RUNS,
    MIN_CONFIRMATION_RUNS,
    SAMPLE_BATCH_SIZE,
    BenchmarkMeasurement,
    benchmark_measurement,
    confirmation_run_count,
    sampling_batch_sizes,
)


print = functools.partial(print, flush=True)

INITIAL_BATCH_SIZE = SAMPLE_BATCH_SIZE
INITIAL_RUNS = 2 * INITIAL_BATCH_SIZE
REGRESSION_LIMIT = 1.10
NOISE_THRESHOLD = 0.02
CONFIDENCE_PERCENTAGE = 95

ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"
ANSI_YELLOW = "\033[33m"
ANSI_GRAY = "\033[90m"
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
    confidence_interval: str
    confidence: str
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
    stop_on_confident_noise: bool = False,
):
    old_timings = []
    new_timings = []
    batch_sizes = sampling_batch_sizes(requested_runs)
    planned_runs = sum(batch_sizes)
    for batch_size in batch_sizes:
        old_batch, old_failure = old_runner.run(benchmark, batch_size)
        new_batch, new_failure = new_runner.run(benchmark, batch_size)

        old_batch = old_batch or []
        new_batch = new_batch or []
        if old_failure or new_failure:
            return old_timings, new_timings, old_failure, new_failure, False
        if len(old_batch) != len(new_batch):
            failure = f"Paired benchmark batches produced different run counts: {len(old_batch)} and {len(new_batch)}"
            return old_timings, new_timings, failure, failure, False

        old_timings.extend(old_batch)
        new_timings.extend(new_batch)
        completed_runs = len(old_timings)
        if stop_on_confident_noise and completed_runs < planned_runs and completed_runs % (2 * SAMPLE_BATCH_SIZE) == 0:
            measurement = benchmark_measurement(old_timings, new_timings)
            lower_bound, upper_bound = measurement.ratio_interval
            if lower_bound >= 1.0 - NOISE_THRESHOLD and upper_bound <= 1.0 + NOISE_THRESHOLD:
                return old_timings, new_timings, None, None, True

    return old_timings, new_timings, None, None, False


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
    old_initial, new_initial, old_failure, new_failure, _ = run_paired_samples(
        old_runner, new_runner, benchmark, INITIAL_RUNS
    )
    initial_count = min(len(old_initial), len(new_initial))
    if old_failure or new_failure:
        return failed_benchmark_result(benchmark, old_failure, new_failure, None, initial_count, 0)

    initial_measurement = benchmark_measurement(old_initial, new_initial)
    is_candidate = (
        initial_measurement.ratio < 1.0 - NOISE_THRESHOLD or initial_measurement.ratio > 1.0 + NOISE_THRESHOLD
    )
    requested_confirmation_runs = confirmation_run_count(initial_measurement) if is_candidate else 0
    if verbose:
        decision = f"confirming with {requested_confirmation_runs} pairs" if is_candidate else "within ±2%; done"
        print(
            f"initial: {benchmark}: {initial_count} pairs | "
            f"median change {format_ratio_change(initial_measurement.ratio)} | {decision}"
        )

    if not is_candidate:
        return BenchmarkResult(
            benchmark=benchmark,
            initial_measurement=initial_measurement,
            measurement=initial_measurement,
            initial_runs=initial_count,
        )

    old_confirmation, new_confirmation, old_failure, new_failure, stopped_early = run_paired_samples(
        old_runner,
        new_runner,
        benchmark,
        requested_confirmation_runs,
        stop_on_confident_noise=True,
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
        else:
            outcome = OUTCOME_UNCONFIRMED_REGRESSION
    else:
        outcome = OUTCOME_NO_REGRESSION

    if verbose:
        print(
            f"confirm: {benchmark}: {confirmation_count} pairs | "
            f"median change {format_ratio_change(confirmation_measurement.ratio)} | "
            f"{CONFIDENCE_PERCENTAGE}% CI {format_ratio_interval((lower_bound, upper_bound))} | "
            f"{measurement_status(confirmation_measurement, outcome, stopped_early)}"
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
    if math.isfinite(value):
        return f"{value:+.1f}%"
    return "-inf%" if value < 0 else "+inf%"


def ratio_change(ratio: float) -> float:
    return (ratio - 1.0) * 100.0


def format_ratio_change(ratio: float) -> str:
    return format_percentage(ratio_change(ratio))


def format_ratio_interval(ratio_interval: Tuple[float, float]) -> str:
    lower_bound, upper_bound = ratio_interval
    return f"{format_ratio_change(lower_bound)}…{format_ratio_change(upper_bound)}"


def direction_confidence(measurement: BenchmarkMeasurement) -> str:
    lower_bound, upper_bound = measurement.ratio_interval
    if measurement.ratio < 1.0 - NOISE_THRESHOLD:
        return "confident" if upper_bound < 1.0 - NOISE_THRESHOLD else "uncertain"
    if measurement.ratio > 1.0 + NOISE_THRESHOLD:
        return "confident" if lower_bound > 1.0 + NOISE_THRESHOLD else "uncertain"
    return "within noise"


def measurement_status(measurement: BenchmarkMeasurement, outcome: str, stopped_early: bool = False) -> str:
    if stopped_early:
        return "within ±2% (confident; stopped early)"
    if outcome == OUTCOME_CONFIRMED_REGRESSION:
        return "regression (confirmed)"
    if outcome == OUTCOME_UNCONFIRMED_REGRESSION:
        return "regression (uncertain; budget exhausted)"
    if measurement.ratio < 1.0 - NOISE_THRESHOLD:
        confidence = direction_confidence(measurement)
        suffix = "confident" if confidence == "confident" else "uncertain; budget exhausted"
        return f"faster ({suffix})"
    if measurement.ratio > 1.0 + NOISE_THRESHOLD:
        confidence = direction_confidence(measurement)
        suffix = "confident" if confidence == "confident" else "uncertain; budget exhausted"
        return f"slower ({suffix})"
    return "within ±2%; no change"


def confidence_text(result: BenchmarkResult) -> str:
    if result.measurement is None:
        return "unavailable"
    return format_ratio_interval(result.measurement.ratio_interval)


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
        delta = "failed"
        median_change = "failed"
        confidence = "unavailable"
        failure_summary.append(result)
    else:
        measurement = result.measurement
        assert measurement is not None
        old_timing = format_seconds(measurement.old_timing)
        new_timing = format_seconds(measurement.new_timing)
        delta = format_delta_seconds(measurement.new_timing - measurement.old_timing)
        median_change = format_ratio_change(measurement.ratio)
        confidence = confidence_text(result)
        message = (
            f"{suite}: {result.benchmark} regressed from {old_timing} to {new_timing}; "
            f"median change {median_change}, delta {delta}, {CONFIDENCE_PERCENTAGE}% CI {confidence}, "
            f"regression limit {format_ratio_change(REGRESSION_LIMIT)}"
        )
    emit_github_error("Regression benchmark", message)
    summary_lines.append(
        f"| `{result.benchmark}` | `{old_timing}` | `{new_timing}` | `{delta}` | `{median_change}` | "
        f"`{confidence}` | `{format_run_count(result)}` |"
    )


def report_unconfirmed(result: BenchmarkResult, suite: str, summary_lines: List[str]):
    measurement = result.measurement
    assert measurement is not None
    old_timing = format_seconds(measurement.old_timing)
    new_timing = format_seconds(measurement.new_timing)
    delta = format_delta_seconds(measurement.new_timing - measurement.old_timing)
    median_change = format_ratio_change(measurement.ratio)
    confidence = confidence_text(result)
    message = (
        f"{suite}: {result.benchmark} has median change {median_change}, delta {delta}, and "
        f"{CONFIDENCE_PERCENTAGE}% CI {confidence}; regression limit {format_ratio_change(REGRESSION_LIMIT)} "
        f"is uncertain after the confirmation budget"
    )
    emit_github_warning("Inconclusive regression benchmark", message)
    summary_lines.append(
        f"| `{result.benchmark}` | `{old_timing}` | `{new_timing}` | `{delta}` | `{median_change}` | "
        f"`{confidence}` | `{format_run_count(result)}` |"
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


def result_confidence(result: BenchmarkResult) -> str:
    if result.outcome == OUTCOME_CONFIRMED_REGRESSION:
        return "confirmed regression"
    if result.outcome == OUTCOME_UNCONFIRMED_REGRESSION:
        return "uncertain regression"
    assert result.measurement is not None
    return direction_confidence(result.measurement)


def benchmark_row(result: BenchmarkResult, display_name: str) -> BenchmarkRow:
    bucket = classify_result(result)
    if result.outcome == OUTCOME_FAILURE:
        return BenchmarkRow(
            result=result,
            display_name=display_name,
            old_timing="failed",
            new_timing="failed",
            delta="failed",
            change="failed",
            confidence_interval="unavailable",
            confidence="failed",
            runs=format_run_count(result),
            bucket=bucket,
        )

    measurement = result.measurement
    assert measurement is not None
    delta = measurement.new_timing - measurement.old_timing
    percentage = ratio_change(measurement.ratio)
    return BenchmarkRow(
        result=result,
        display_name=display_name,
        old_timing=format_seconds(measurement.old_timing),
        new_timing=format_seconds(measurement.new_timing),
        delta=format_delta_seconds(delta),
        change=format_percentage(percentage),
        confidence_interval=format_ratio_interval(measurement.ratio_interval),
        confidence=result_confidence(result),
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
    return f"{ANSI_GRAY}{value}{ANSI_RESET}"


def gray(value: str) -> str:
    return f"{ANSI_GRAY}{value}{ANSI_RESET}"


def color_result(result_text: str) -> str:
    if result_text == "no regressions":
        color = ANSI_GREEN
    elif result_text == "uncertain regressions":
        color = ANSI_YELLOW
    else:
        color = ANSI_RED
    return f"{color}{result_text}{ANSI_RESET}"


def color_geomean_change(percentage: float) -> str:
    value = format_percentage(percentage)
    if percentage < 0:
        return f"{ANSI_GREEN}{value}{ANSI_RESET}"
    if percentage > 0:
        return f"{ANSI_RED}{value}{ANSI_RESET}"
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
    headers = ["benchmark", "base", "PR", "delta", "median change", "95% CI", "confidence", "runs"]
    plain_rows = [
        [
            row.display_name,
            row.old_timing,
            row.new_timing,
            row.delta,
            row.change,
            row.confidence_interval,
            row.confidence,
            row.runs,
        ]
        for row in rows
    ]
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
            if headers[index] == "median change":
                padded = color_change(row.bucket, padded)
            cells.append(padded)
        print("  ".join(cells))


def print_bucket(title: str, rows: List[BenchmarkRow], unchanged_count: int = 0):
    if title == "UNCHANGED (±2%)":
        if not unchanged_count:
            return
    elif not rows:
        return
    print("")
    if title == "UNCHANGED (±2%)":
        print(gray(title))
        print(gray(f"{unchanged_count} benchmarks"))
    else:
        print(title)
        render_table(rows)


def geomean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return math.exp(math.fsum(math.log(value) for value in values) / len(values))


def print_geomean_summary(old_geomean: Optional[float], new_geomean: Optional[float]):
    print("")
    sample_text = gray(f"(initial {INITIAL_RUNS} samples)")
    if old_geomean is None or new_geomean is None:
        print(f"geomean: unavailable  {sample_text}")
        return
    percentage = ((new_geomean / old_geomean) - 1.0) * 100.0
    change = color_geomean_change(percentage)
    print(f"geomean: {format_seconds(old_geomean)} -> {format_seconds(new_geomean)}  " f"{change}  {sample_text}")


def print_benchmark_report(
    rows: List[BenchmarkRow],
    common_prefix: str,
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

    print("")
    suite_text = f"{common_prefix} ({len(rows)} benchmarks)" if common_prefix else f"{len(rows)} benchmarks"
    print(gray(f"suite: {suite_text}"))
    print(
        gray(
            f"sampling: {INITIAL_RUNS} initial pairs; {MIN_CONFIRMATION_RUNS}–{MAX_CONFIRMATION_RUNS} "
            f"confirmation pairs outside ±{NOISE_THRESHOLD * 100:.0f}%, with early noise stop"
        )
    )
    print(
        gray(
            f"regression: fail when the {CONFIDENCE_PERCENTAGE}% CI is above "
            f"{format_ratio_change(REGRESSION_LIMIT)}"
        )
    )
    print_bucket("UNCHANGED (±2%)", buckets[BUCKET_UNCHANGED], len(buckets[BUCKET_UNCHANGED]))
    print_bucket("FASTER", buckets[BUCKET_FASTER])
    print_bucket("SLOWER (+2%…+10%)", buckets[BUCKET_SLOWER])
    print_bucket("UNCERTAIN REGRESSIONS", buckets[BUCKET_UNCONFIRMED])
    print_bucket("REGRESSIONS", buckets[BUCKET_REGRESSION])
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
            "| Benchmark | Base | PR | Delta | Median change | 95% CI | Runs |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for result in failing_results:
            report_regression(result, suite, failure_summary, summary_lines)
        append_step_summary(summary_lines + [""])

    if inconclusive_results:
        summary_lines = [
            f"## Inconclusive Regression Candidates: `{suite}`",
            "",
            "| Benchmark | Base | PR | Delta | Median change | 95% CI | Runs |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for result in inconclusive_results:
            report_unconfirmed(result, suite, summary_lines)
        append_step_summary(summary_lines + [""])

    display_names = benchmark_display_names([result.benchmark for result in results])
    rows = [benchmark_row(result, display_names[result.benchmark]) for result in results]
    rows.sort(key=row_sort_key)
    if any(result.outcome == OUTCOME_FAILURE for result in results):
        result_text = "benchmark failure"
    elif any(result.outcome == OUTCOME_CONFIRMED_REGRESSION for result in results):
        result_text = "regression detected"
    elif inconclusive_results:
        result_text = "uncertain regressions"
    else:
        result_text = "no regressions"

    initial_measurements = [result.initial_measurement for result in results if result.initial_measurement]
    old_geomean = geomean([measurement.old_timing for measurement in initial_measurements])
    new_geomean = geomean([measurement.new_timing for measurement in initial_measurements])
    print_benchmark_report(
        rows,
        benchmark_common_prefix([result.benchmark for result in results]),
    )
    print_failure_summary(failure_summary)
    print_geomean_summary(old_geomean, new_geomean)
    print(f"result: {color_result(result_text)}")
    return 1 if failing_results else 0


if __name__ == "__main__":
    raise SystemExit(main())
