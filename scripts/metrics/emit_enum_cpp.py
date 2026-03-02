from __future__ import annotations

from typing import Dict, List, Tuple
from pathlib import Path

from .inputs import _to_pascal_case
from .model import MetricIndex
from .writer import IndentedFileWriter, write_warning

HPP_HEADER = """

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"

namespace duckdb {

"""

HPP_TYPEDEFS = """
struct MetricTypeHashFunction {
    uint64_t operator()(const MetricType &index) const {
        return std::hash<uint8_t>()(static_cast<uint8_t>(index));
    }
};

typedef unordered_set<MetricType, MetricTypeHashFunction> profiler_settings_t;
typedef unordered_map<MetricType, Value, MetricTypeHashFunction> profiler_metrics_t;

"""

CPP_HEADER = """
#include "duckdb/common/enums/metric_type.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

"""


def _setup_hpp(out_hpp: Path, f: IndentedFileWriter, metric_index: MetricIndex):
    f.write_header("duckdb/common/enums/metric_type.hpp")
    f.write(HPP_HEADER)

    f.write("enum class MetricGroup : uint8_t {\n")

    groups = metric_index.group_names + ["INVALID"]
    for g in groups:
        f.write_indented(1, f"{g.upper()},")
    f.write("};\n\n")

    f.write("enum class MetricType : uint8_t {\n")
    for g in metric_index.group_names:
        if g == "all" or g == "default":
            continue

        f.write_indented(1, f"// {_to_pascal_case(g)} metrics")
        for m in metric_index.metrics_per_group(g):
            val = metric_index.metric_value(m)
            if val is None:
                raise ValueError(f"Metric '{m}' in group '{g}' has no explicit value assigned")
            f.write_indented(1, f"{m.upper()} = {val},")

    f.write("};\n")

    f.write(HPP_TYPEDEFS)
    f.write('class MetricsUtils {\n')
    f.write('public:\n')
    opt_metrics = metric_index.metrics_per_group("optimizer")
    if opt_metrics:
        f.write_indented(
            1,
            f"static constexpr uint8_t START_OPTIMIZER = static_cast<uint8_t>(MetricType::{opt_metrics[0]});",
        )
        f.write_indented(
            1,
            f"static constexpr uint8_t END_OPTIMIZER = static_cast<uint8_t>(MetricType::{opt_metrics[-1]});\n",
        )
    f.write('public:\n')


def _generate_standard_functions(
    group: str, hpp_f: IndentedFileWriter, cpp_f: IndentedFileWriter, metric_index: MetricIndex
):
    formatted = _to_pascal_case(group)
    get_fn = f"Get{formatted}Metrics"

    hpp_f.write('\n')
    hpp_f.write_indented(1, f"// {formatted} metrics")
    hpp_f.write_indented(1, f"static profiler_settings_t {get_fn}();")

    metrics = metric_index.metrics_per_group(group) if group != "root_scope" else metric_index.root_scope_metrics()

    cpp_f.write(f"profiler_settings_t MetricsUtils::{get_fn}() {{\n")

    if group == "optimizer":
        cpp_f.write_indented(1, "profiler_settings_t result;")
        cpp_f.write_indented(1, f"for (auto metric = START_OPTIMIZER; metric <= END_OPTIMIZER; metric++) {{")
        cpp_f.write_indented(2, f"result.insert(static_cast<MetricType>(metric));")
        cpp_f.write_indented(1, "}")
        cpp_f.write_indented(1, "return result;")
    else:
        cpp_f.write_indented(1, "return {")
        for m in metrics:
            cpp_f.write_indented(2, f"MetricType::{m},")
        cpp_f.write_indented(1, "};")
    cpp_f.write('}\n\n')

    if group == "all":
        _generate_get_metric_by_group_function(hpp_f, cpp_f, metric_index)
        return

    check_fn = f"Is{formatted}Metric"
    hpp_f.write_indented(1, f"static bool {check_fn}(MetricType type);")

    cpp_f.write(f"bool MetricsUtils::{check_fn}(MetricType type) {{\n")
    if group == "optimizer":
        cpp_f.write_indented(
            1,
            f"return static_cast<uint8_t>(type) >= START_OPTIMIZER && static_cast<uint8_t>(type) <= END_OPTIMIZER;",
        )
    else:
        cpp_f.write_indented(1, "switch(type) {")
        for m in metrics:
            cpp_f.write_indented(1, f"case MetricType::{m}:")
        cpp_f.write_indented(2, "return true;")
        cpp_f.write_indented(1, "default:")
        cpp_f.write_indented(2, "return false;")
        cpp_f.write_indented(1, "}")
    cpp_f.write("}\n\n")


def _generate_custom_optimizer_functions(
    optimizers: List[Tuple[str, int]], hpp_f: IndentedFileWriter, cpp_f: IndentedFileWriter
):
    by_type = "GetOptimizerMetricByType(OptimizerType type)"
    by_metric = "GetOptimizerTypeByMetric(MetricType type)"

    hpp_f.write_indented(1, f"static MetricType {by_type};")
    hpp_f.write_indented(1, f"static OptimizerType {by_metric};")

    first_optimizer = optimizers[0][0]

    cpp_f.write(
        f"""
MetricType MetricsUtils::GetOptimizerMetricByType(OptimizerType type) {{
	if (type == OptimizerType::INVALID) {{
		throw InternalException("Invalid OptimizerType: INVALID");
	}}

	const auto base_opt = static_cast<uint8_t>(OptimizerType::{first_optimizer});
	const auto idx = static_cast<uint8_t>(type) - base_opt;

	const auto metric_u8 = static_cast<uint8_t>(START_OPTIMIZER + idx);
	if (metric_u8 < START_OPTIMIZER || metric_u8 > END_OPTIMIZER) {{
		throw InternalException("OptimizerType out of MetricType optimizer range");
	}}
	return static_cast<MetricType>(metric_u8);
}}

OptimizerType MetricsUtils::GetOptimizerTypeByMetric(MetricType type) {{
	const auto metric_u8 = static_cast<uint8_t>(type);
	if (!IsOptimizerMetric(type)) {{
		throw InternalException("MetricType is not an optimizer metric");
	}}

	const auto idx = static_cast<uint8_t>(metric_u8 - START_OPTIMIZER);
	const auto result = static_cast<uint8_t>(OptimizerType::{first_optimizer}) + idx;
	return static_cast<OptimizerType>(result);
}}

"""
    )


def _generate_get_metric_by_group_function(
    hpp_f: IndentedFileWriter, cpp_f: IndentedFileWriter, metric_index: MetricIndex
):
    fn = "GetMetricsByGroupType(MetricGroup type)"
    hpp_f.write_indented(1, f"static profiler_settings_t {fn};")

    cpp_f.write(f"profiler_settings_t MetricsUtils::{fn} {{\n")
    cpp_f.write_indented(1, "switch(type) {")
    for group in metric_index.group_names:
        formatted = group.upper()
        cpp_f.write_indented(1, f"case MetricGroup::{formatted}:")
        cpp_f.write_indented(2, "return Get" + _to_pascal_case(group) + "Metrics();")
    cpp_f.write_indented(1, "default:")
    cpp_f.write_indented(2, 'throw InternalException("The MetricGroup passed is invalid");')
    cpp_f.write_indented(1, "}")
    cpp_f.write('}\n\n')


def generate_metric_type_files(
    out_hpp: Path,
    out_cpp: Path,
    metric_index: MetricIndex,
    optimizers: List[Tuple[str, int]],
) -> None:
    with IndentedFileWriter(out_hpp) as hpp_f, IndentedFileWriter(out_cpp) as cpp_f:
        _setup_hpp(out_hpp, hpp_f, metric_index)
        cpp_f.write(write_warning())
        cpp_f.write(CPP_HEADER)

        for group in metric_index.metrics_by_group:
            _generate_standard_functions(group, hpp_f, cpp_f, metric_index)
            if group == "optimizer":
                _generate_custom_optimizer_functions(optimizers, hpp_f, cpp_f)

        _generate_standard_functions("root_scope", hpp_f, cpp_f, metric_index)

        hpp_f.write("};\n")
        hpp_f.write("} // namespace duckdb\n")
        cpp_f.write("}\n")
