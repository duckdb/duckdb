import math
import statistics
from dataclasses import dataclass
from typing import List, Tuple


CONFIRMATION_TARGET_SECONDS = 3.0
MIN_CONFIRMATION_RUNS = 30
MAX_CONFIRMATION_RUNS = 100
SAMPLE_BATCH_SIZE = 5


@dataclass
class BenchmarkMeasurement:
    old_timing: float
    new_timing: float
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


def benchmark_measurement(old_timings: List[float], new_timings: List[float]) -> BenchmarkMeasurement:
    if not old_timings or len(old_timings) != len(new_timings):
        raise ValueError("Paired benchmark timings must be non-empty and have equal length")
    if any(not math.isfinite(timing) or timing <= 0 for timing in old_timings + new_timings):
        raise ValueError("Benchmark timings must be finite and greater than zero")

    ratios = [new_timing / old_timing for old_timing, new_timing in zip(old_timings, new_timings)]
    return BenchmarkMeasurement(
        old_timing=float(statistics.median(old_timings)),
        new_timing=float(statistics.median(new_timings)),
        ratio=float(statistics.median(ratios)),
        ratio_interval=median_confidence_interval(ratios),
        runs=len(ratios),
    )


def confirmation_run_count(measurement: BenchmarkMeasurement) -> int:
    faster_side_median = min(measurement.old_timing, measurement.new_timing)
    estimated_batches = math.ceil(CONFIRMATION_TARGET_SECONDS / faster_side_median / SAMPLE_BATCH_SIZE)
    estimated_runs = estimated_batches * SAMPLE_BATCH_SIZE
    return max(MIN_CONFIRMATION_RUNS, min(MAX_CONFIRMATION_RUNS, estimated_runs))


def sampling_batch_sizes(requested_runs: int) -> List[int]:
    if requested_runs <= 0:
        raise ValueError("Requested benchmark runs must be greater than zero")
    batch_count = math.ceil(requested_runs / SAMPLE_BATCH_SIZE)
    return [SAMPLE_BATCH_SIZE] * batch_count
