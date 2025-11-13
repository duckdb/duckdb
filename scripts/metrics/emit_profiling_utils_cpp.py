from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Callable

from .inputs import retrieve_template
from .model import MetricIndex
from .paths import PROFILING_HPP_TEMPLATE, PROFILING_CPP_TEMPLATE
from .writer import IndentedFileWriter, write_warning

HPP_HEADER = """#pragma once

#include "duckdb/common/enums/metric_type.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/profiling_node.hpp"

namespace duckdb_yyjson {
struct yyjson_mut_doc;
struct yyjson_mut_val;
} // namespace duckdb_yyjson

namespace duckdb {

"""

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
    cpp_f.write(f"void {class_name}::{function_name} {{")
    cpp_f.write("switch(type) {")
    for t in types:
        for m in types[t]:
            cpp_f.write(f"case MetricType::{m}:")

        res = case_logic(t)
        if res is not None:
            cpp_f.write(res)
        cpp_f.write("break;")

    cpp_f.write("default:")
    cpp_f.write("throw InternalException(\"Unknown metric type %s\", EnumUtil::ToString(type));")
    cpp_f.write("}")
    cpp_f.write("}\n")


def _generate_collection_methods(
    cpp_f: IndentedFileWriter, class_name: str, function_name: str, metric_index: MetricIndex
) -> None:
    cpp_f.write(f"void {class_name}::{function_name} {{")
    cpp_f.write("switch(type) {")
    for c in metric_index.collection_index():
        for m in metric_index.metrics_per_collection(c):
            cpp_f.write(f"case MetricType::{m}:")
            if c == "timer":
                cpp_f.write(f"metric = query_metrics.{m.lower()}.Elapsed();")
            elif c == "child":
                cpp_f.write(f"metric = child_info.metrics[MetricType::{metric_index.metric_child(m)}];")
            elif c == "cumulative_operators":
                cpp_f.write(f"metric = GetCumulativeOptimizers(node);")
            elif c == "query_metric" and metric_index.metric_type(m) == "uint64_t":
                cpp_f.write(f"metric = Value::UBIGINT(query_metrics.{m.lower()});")
            elif c == "cumulative":
                cpp_f.write(
                    f"GetCumulativeMetric<{metric_index.metric_type(m)}>(node, MetricType::{m}, MetricType::{metric_index.metric_child(m)});",
                )
            else:
                raise Exception(f"Unknown collection type {c} or metric {m}")
            cpp_f.write(f"break;")

    cpp_f.write("default:")
    cpp_f.write("return;")
    cpp_f.write("}")
    cpp_f.write("}\n")


def generate_profiling_utils(
    out_hpp: Path,
    out_cpp: Path,
    metric_index: MetricIndex,
) -> None:
    with IndentedFileWriter(out_hpp) as hpp_f, IndentedFileWriter(out_cpp) as cpp_f:
        hpp_f.write_header("duckdb/main/profiling_utils.hpp")
        hpp_f.write(retrieve_template(PROFILING_HPP_TEMPLATE))
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

        cpp_f.write("}")
