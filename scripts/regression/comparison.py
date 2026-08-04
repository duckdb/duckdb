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
class SamplingDecision:
    collect_more: bool
    reason: str


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


def paired_measurement(old_timings: List[float], new_timings: List[float]) -> PairedBenchmarkMeasurement:
    if not old_timings or len(old_timings) != len(new_timings):
        raise ValueError("Paired benchmark timings must be non-empty and have equal length")
    if any(not math.isfinite(timing) or timing <= 0 for timing in old_timings + new_timings):
        raise ValueError("Benchmark timings must be finite and greater than zero")

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


def sampling_decision(
    measurement: PairedBenchmarkMeasurement,
    measured_seconds: float,
    minimum_runs: int,
    maximum_runs: int,
    maximum_adaptive_seconds: float,
    display_threshold_percentage: float,
    regression_threshold_percentage: float,
    regression_threshold_seconds: float,
) -> SamplingDecision:
    if measurement.runs < minimum_runs:
        return SamplingDecision(True, "minimum timed runs not reached")
    if measurement.runs >= maximum_runs:
        return SamplingDecision(False, "maximum timed runs reached")
    if measured_seconds >= maximum_adaptive_seconds:
        return SamplingDecision(False, "adaptive timing budget reached")

    display_ratio = display_threshold_percentage / 100.0
    change = measurement.ratio - 1.0
    lower_ratio, upper_ratio = measurement.ratio_interval
    if abs(change) <= display_ratio:
        return SamplingDecision(False, "old and new timings are within the display threshold")

    if change < 0:
        if upper_ratio < 1.0 - display_ratio:
            return SamplingDecision(False, "improvement is statistically stable")
        return SamplingDecision(True, "visible improvement is statistically uncertain")

    regression_ratio = (
        (measurement.old_timing + regression_threshold_seconds) * (1.0 + regression_threshold_percentage)
    ) / measurement.old_timing
    if measurement.ratio > regression_ratio:
        if lower_ratio > regression_ratio:
            return SamplingDecision(False, "regression is statistically stable")
        return SamplingDecision(True, "regression is statistically uncertain")

    if lower_ratio > 1.0 + display_ratio:
        return SamplingDecision(False, "slowdown is statistically stable")
    return SamplingDecision(True, "visible slowdown is statistically uncertain")
