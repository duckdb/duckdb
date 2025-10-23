from scripts.metrics.emit_cpp import generate_metric_type_files
from scripts.metrics.emit_tests import generate_test_files
from scripts.metrics.inputs import load_metrics_json, validate_identifier, retrieve_optimizers
from scripts.metrics.model import build_all_metrics
from scripts.metrics.paths import METRICS_JSON, OPTIMIZER_HPP, OUT_METRIC_HPP, OUT_METRIC_CPP, TEST_PROFILING_DIR


def main():
    # load metrics from JSON
    metrics_json = load_metrics_json(METRICS_JSON)

    for group in metrics_json:
        if "metrics" not in group:
            continue
        for metric in group["metrics"]:
            validate_identifier(metric["name"], group["group"])

    optimizers = retrieve_optimizers(OPTIMIZER_HPP)

    all_metrics, root_scope = build_all_metrics(metrics_json, optimizers)

    # emit C++ files
    generate_metric_type_files(OUT_METRIC_HPP, OUT_METRIC_CPP, all_metrics, optimizers, root_scope)

    # emit test files
    generate_test_files(TEST_PROFILING_DIR, all_metrics)


if __name__ == "__main__":
    main()
