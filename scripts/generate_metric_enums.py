from metrics.emit_enum_cpp import generate_metric_type_files
from metrics.emit_profiling_utils_cpp import generate_profiling_utils
from metrics.emit_tests import generate_test_files
from metrics.inputs import load_metrics_json, retrieve_optimizers
from metrics.model import build_all_metrics
from metrics.paths import (
    METRICS_JSON,
    OPTIMIZER_HPP,
    OUT_METRIC_HPP,
    OUT_METRIC_CPP,
    TEST_PROFILING_DIR,
    OUT_PROFILING_HPP,
    OUT_PROFILING_CPP,
)


def main():
    # load metrics from JSON
    metrics_json = load_metrics_json(METRICS_JSON)

    optimizers = retrieve_optimizers(OPTIMIZER_HPP)

    metric_index = build_all_metrics(metrics_json, optimizers)

    # emit C++ files
    generate_metric_type_files(OUT_METRIC_HPP, OUT_METRIC_CPP, metric_index, optimizers)
    generate_profiling_utils(OUT_PROFILING_HPP, OUT_PROFILING_CPP, metric_index)

    # emit test files
    generate_test_files(TEST_PROFILING_DIR, metric_index.metrics_by_group)


if __name__ == "__main__":
    main()
