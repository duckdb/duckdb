import math
import statistics
from dataclasses import dataclass
from typing import List


CONFIRMATION_TARGET_SECONDS = 3.0
MIN_CONFIRMATION_RUNS = 30
MAX_CONFIRMATION_RUNS = 100
SAMPLE_BATCH_SIZE = 5


@dataclass
class BenchmarkMeasurement:
    old_timing: float
    old_min: float
    old_max: float
    new_timing: float
    new_min: float
    new_max: float
    ratio: float
    runs: int


def benchmark_measurement(old_timings: List[float], new_timings: List[float]) -> BenchmarkMeasurement:
    if not old_timings or len(old_timings) != len(new_timings):
        raise ValueError("Paired benchmark timings must be non-empty and have equal length")
    if any(not math.isfinite(timing) or timing <= 0 for timing in old_timings + new_timings):
        raise ValueError("Benchmark timings must be finite and greater than zero")

    old_timing = float(statistics.median(old_timings))
    new_timing = float(statistics.median(new_timings))
    return BenchmarkMeasurement(
        old_timing=old_timing,
        old_min=min(old_timings),
        old_max=max(old_timings),
        new_timing=new_timing,
        new_min=min(new_timings),
        new_max=max(new_timings),
        ratio=new_timing / old_timing,
        runs=len(old_timings),
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
