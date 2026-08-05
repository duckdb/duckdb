import math
import statistics
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class PairedBenchmarkMeasurement:
    old_timing: float
    new_timing: float
    ratio: float
    ratio_interval: Tuple[float, float]
    runs: int


@dataclass
class RegressionMeasurement:
    ratio: float
    ratio_interval: Tuple[float, float]
    runs: int


def median_confidence_interval(values: List[float], confidence: float = 0.95) -> Tuple[float, float]:
    """Return a distribution-free confidence interval for the population median."""
    if not values:
        raise ValueError("Cannot compute a confidence interval without values")

    ordered = sorted(values)
    count = len(ordered)
    lower_order = None
    for order in range(1, count // 2 + 1):
        tail_probability = sum(math.comb(count, index) for index in range(order)) / (2**count)
        coverage = 1.0 - 2.0 * tail_probability
        if coverage >= confidence:
            lower_order = order

    if lower_order is None:
        return -math.inf, math.inf
    return ordered[lower_order - 1], ordered[count - lower_order]


def validate_paired_timings(old_timings: List[float], new_timings: List[float]):
    if not old_timings or len(old_timings) != len(new_timings):
        raise ValueError("Paired benchmark timings must be non-empty and have equal length")
    if any(not math.isfinite(timing) or timing <= 0 for timing in old_timings + new_timings):
        raise ValueError("Benchmark timings must be finite and greater than zero")


def paired_measurement(old_timings: List[float], new_timings: List[float]) -> PairedBenchmarkMeasurement:
    validate_paired_timings(old_timings, new_timings)

    ratios = [new_timing / old_timing for old_timing, new_timing in zip(old_timings, new_timings)]
    old_timing = float(statistics.median(old_timings))
    ratio = float(statistics.median(ratios))
    return PairedBenchmarkMeasurement(
        old_timing=old_timing,
        new_timing=old_timing * ratio,
        ratio=ratio,
        ratio_interval=median_confidence_interval(ratios),
        runs=len(ratios),
    )


def regression_measurement(
    old_timings: List[float],
    new_timings: List[float],
    regression_threshold_percentage: float,
    regression_threshold_seconds: float,
) -> RegressionMeasurement:
    validate_paired_timings(old_timings, new_timings)
    if regression_threshold_percentage < 0 or regression_threshold_seconds < 0:
        raise ValueError("Regression thresholds must not be negative")

    threshold_multiplier = 1.0 + regression_threshold_percentage
    ratios = [
        new_timing / ((old_timing + regression_threshold_seconds) * threshold_multiplier)
        for old_timing, new_timing in zip(old_timings, new_timings)
    ]
    return RegressionMeasurement(
        ratio=float(statistics.median(ratios)),
        ratio_interval=median_confidence_interval(ratios),
        runs=len(ratios),
    )


def confirmation_run_count(
    measurement: PairedBenchmarkMeasurement,
    target_seconds: float,
    minimum_runs: int,
    maximum_runs: int,
) -> int:
    if target_seconds <= 0:
        raise ValueError("Confirmation time must be greater than zero")
    if minimum_runs <= 0 or maximum_runs < minimum_runs:
        raise ValueError("Invalid confirmation run bounds")

    faster_timing = min(measurement.old_timing, measurement.new_timing)
    estimated_runs = math.ceil(target_seconds / faster_timing)
    return max(minimum_runs, min(maximum_runs, estimated_runs))
