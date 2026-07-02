from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Callable

from .inputs import retrieve_template, START_OF_FILE, INSERT_CODE_HERE
from .model import MetricIndex
from .paths import PROFILING_HPP_TEMPLATE, PROFILING_CPP_TEMPLATE
from .writer import IndentedFileWriter, write_warning

CPP_HEADER = """
#include "duckdb/main/profiling_utils.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/main/profiling_node.hpp"
#include "duckdb/main/query_profiler.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {"""


def _default_case_logic(t: str) -> Optional[str]:
    val = "Value::"
    if t == "Value::MAP":
        val += "MAP(InsertionOrderPreservingMap<string>())"
    else:
        val += "CreateValue"
        if t == "string":
            val += "(\"\")"
        elif t == "double":
            val += "(0.0)"
        elif t == "uint64":
            val += "<uint64_t>(0)"
        elif t == "uint8":
            val += "<uint8_t>(0)"

    return f"metrics[type] = {val};"


def _metric_to_json_case_logic(t: str) -> Optional[str]:
    if t == "map":
        return None
    elif t == "string":
        return "yyjson_mut_obj_add_strcpy(doc, dest, key_ptr, metrics[type].GetValue<string>().c_str());"
    elif t == "double":
        return "yyjson_mut_obj_add_real(doc, dest, key_ptr, metrics[type].GetValue<double>());"
    elif t == "uint64":
        return "yyjson_mut_obj_add_uint(doc, dest, key_ptr, metrics[type].GetValue<uint64_t>());"
    elif t == "uint8":
        return "yyjson_mut_obj_add_strcpy(doc, dest, key_ptr, OperatorToString(metrics[type]).c_str());"
    return None


def _write_function(
    cpp_f: IndentedFileWriter,
    types: Dict[str, list[str]],
    class_name: str,
    function_name: str,
    case_logic: Callable[[str], Optional[str]],
) -> None:
    # set metric to default
    cpp_f.write_indented(0, f"void {class_name}::{function_name} {{")
    cpp_f.write_indented(1, "switch(type) {")
    for t in types:
        for m in types[t]:
            cpp_f.write_indented(1, f"case MetricType::{m}:")

        res = case_logic(t)
        if res is not None:
            cpp_f.write_indented(2, res)
        cpp_f.write_indented(2, "break;")

    cpp_f.write_indented(1, "default:")
    cpp_f.write_indented(2, "throw InternalException(\"Unknown metric type %s\", EnumUtil::ToString(type));")
    cpp_f.write_indented(1, "}")
    cpp_f.write_indented(0, "}\n")


def _generate_collection_methods(
    cpp_f: IndentedFileWriter, class_name: str, function_name: str, metric_index: MetricIndex
) -> None:
    cpp_f.write_indented(0, f"void {class_name}::{function_name} {{")
    cpp_f.write_indented(1, "switch(type) {")
    for c in metric_index.collection_index():
        for m in metric_index.metrics_per_collection(c):
            cpp_f.write_indented(1, f"case MetricType::{m}:")
            if c == "timer":
                cpp_f.write_indented(2, f"metric = Value::DOUBLE(query_metrics.GetMetricInSeconds(MetricType::{m}));")
            elif c == "child":
                cpp_f.write_indented(2, f"metric = child_info.metrics[MetricType::{metric_index.metric_child(m)}];")
            elif c == "cumulative_operators":
                cpp_f.write_indented(2, f"metric = GetCumulativeOptimizers(node);")
            elif c == "query_metric":
                if metric_index.metric_type(m) == "uint64_t":
                    cpp_f.write_indented(2, f"metric = Value::UBIGINT(query_metrics.GetMetricValue(MetricType::{m}));")
                elif metric_index.metric_type(m) == "string":
                    cpp_f.write_indented(2, f"metric = query_metrics.{m.lower()};")
            elif c == "cumulative":
                cpp_f.write_indented(
                    2,
                    f"GetCumulativeMetric<{metric_index.metric_type(m)}>(node, MetricType::{m}, MetricType::{metric_index.metric_child(m)});",
                )
            else:
                raise Exception(f"Unknown collection type {c} or metric {m}")
            cpp_f.write_indented(2, f"break;")

    cpp_f.write_indented(1, "default:")
    cpp_f.write_indented(2, "return;")
    cpp_f.write_indented(1, "}")
    cpp_f.write_indented(0, "}\n")


def _write_query_metric_functions(
    hpp_f: IndentedFileWriter, query_metric_types: list[tuple[str, str, str]], f_name: str, group: str, action: str
) -> None:
    hpp_f.write_indented(1, f"void {f_name} {{")
    hpp_f.write_indented(2, "switch(type) {")
    for m, t, d in query_metric_types:
        if t == group:
            hpp_f.write_indented(2, f"case MetricType::{m}:")
            a = action
            m_lower = m.lower()
            if action == "GEN_FROM_METRIC":
                a = f".store({m_lower}.load() + amount)"
            hpp_f.write_indented(3, f"{m_lower}{a};")
            hpp_f.write_indented(3, "break;")
    hpp_f.write_indented(2, "default:")
    hpp_f.write_indented(3, "return;")
    hpp_f.write_indented(2, "};")
    hpp_f.write_indented(1, "}\n")


def _generate_query_metrics(hpp_f: IndentedFileWriter, metric_index: MetricIndex) -> None:
    query_metric_types: list[str] = []

    # if the collection method is timer or query_metric then add to query_metrics
    for c in metric_index.collection_index():
        for m in metric_index.metrics_per_collection(c):
            if m.lower() != "query_name" and (c == "timer" or c == "query_metric"):
                query_metric_types.append(m)

    hpp_f.write_indented(1, "static idx_t GetMetricsIndex(MetricType type) {")
    hpp_f.write_indented(2, "switch(type) {")
    for i, m in enumerate(query_metric_types):
        hpp_f.write_indented(2, f"case MetricType::{m}: return {i};")
    hpp_f.write_indented(2, "default:")
    hpp_f.write_indented(
        3, "throw InternalException(\"MetricType %s is not actively tracked.\", EnumUtil::ToString(type));"
    )
    hpp_f.write_indented(2, "}")
    hpp_f.write_indented(1, "}")

    hpp_f.write_indented(0, "\nprivate:")
    hpp_f.write_indented(1, f"static constexpr const idx_t ACTIVELY_TRACKED_METRICS = {len(query_metric_types)};")


def generate_profiling_utils(
    out_hpp: Path,
    out_cpp: Path,
    metric_index: MetricIndex,
) -> None:
    with IndentedFileWriter(out_hpp) as hpp_f, IndentedFileWriter(out_cpp) as cpp_f:

        hpp_f.write_header("duckdb/main/profiling_utils.hpp")
        hpp_f.write(retrieve_template(PROFILING_HPP_TEMPLATE, START_OF_FILE, INSERT_CODE_HERE))
        _generate_query_metrics(hpp_f, metric_index)
        hpp_f.write(retrieve_template(PROFILING_HPP_TEMPLATE, INSERT_CODE_HERE))

        cpp_f.write(write_warning())
        cpp_f.write(CPP_HEADER)
        cpp_f.write(retrieve_template(PROFILING_CPP_TEMPLATE))

        class_name = "ProfilingUtils"

        default_function = "SetMetricToDefault(profiler_metrics_t &metrics, const MetricType &type)"
        metric_to_json = "MetricToJson(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *dest, const char *key_ptr,  profiler_metrics_t &metrics, const MetricType &type)"
        collect_metrics = "CollectMetrics(const MetricType &type, QueryMetrics &query_metrics, Value &metric, ProfilingNode &node, ProfilingInfo &child_info)"

        _write_function(cpp_f, metric_index.types_index(), class_name, default_function, _default_case_logic)
        _write_function(cpp_f, metric_index.types_index(), class_name, metric_to_json, _metric_to_json_case_logic)

        _generate_collection_methods(cpp_f, class_name, collect_metrics, metric_index)

        cpp_f.write_indented(0, "}")
