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
    path_from_duckdb,
    format_file,
)

if __name__ == "__main__":
    # load metrics from JSON
    metrics_json = load_metrics_json(METRICS_JSON)

    optimizers = retrieve_optimizers(OPTIMIZER_HPP)

    metric_index = build_all_metrics(metrics_json, optimizers)

    print(f"Metric Information:")
    print(f"  * Total Metrics: {len(metric_index.metrics_by_group['all'])}")
    print(f"  * Default Metrics: {len(metric_index.metrics_by_group['default'])}")
    print(f"  * Total Groups: {len(metric_index.group_names)}")
    print(f"  * Metric Groups and Total Metrics per Group:")
    for group in metric_index.group_names:
        if group == "all" or group == "default":
            continue
        print(f"    * {group}: {len(metric_index.metrics_by_group[group])}")

    print("\nGenerating files:")
    # emit C++ files
    generate_metric_type_files(OUT_METRIC_HPP, OUT_METRIC_CPP, metric_index, optimizers)
    print(f"  * {path_from_duckdb(OUT_METRIC_HPP)}")
    format_file(OUT_METRIC_HPP)
    print(f"  * {path_from_duckdb(OUT_METRIC_CPP)}")
    format_file(OUT_METRIC_CPP)

    generate_profiling_utils(OUT_PROFILING_HPP, OUT_PROFILING_CPP, metric_index)
    print(f"  * {path_from_duckdb(OUT_PROFILING_HPP)}")
    format_file(OUT_PROFILING_HPP)
    print(f"  * {path_from_duckdb(OUT_PROFILING_CPP)}")
    format_file(OUT_PROFILING_CPP)

    print("\nGenerating tests:")
    # emit test files
    generate_test_files(TEST_PROFILING_DIR, metric_index.metrics_by_group)
