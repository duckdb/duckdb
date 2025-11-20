from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Callable

from .inputs import retrieve_template, START_OF_FILE, INSERT_CODE_HERE
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
                cpp_f.write_indented(2, f"metric = query_metrics.{m.lower()}.Elapsed();")
            elif c == "child":
                cpp_f.write_indented(2, f"metric = child_info.metrics[MetricType::{metric_index.metric_child(m)}];")
            elif c == "cumulative_operators":
                cpp_f.write_indented(2, f"metric = GetCumulativeOptimizers(node);")
            elif c == "query_metric":
                if metric_index.metric_type(m) == "uint64_t":
                    cpp_f.write_indented(2, f"metric = Value::UBIGINT(query_metrics.{m.lower()});")
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
            hpp_f.write_indented(3, f"{m.lower()}{action};")
            hpp_f.write_indented(3, "break;")
    hpp_f.write_indented(2, "default:")
    hpp_f.write_indented(3, "return;")
    hpp_f.write_indented(2, "};")
    hpp_f.write_indented(1, "}\n")


def _generate_query_metrics(hpp_f: IndentedFileWriter, metric_index: MetricIndex) -> None:
    query_metric_types: list[tuple[str, str, str]] = []

    # if the collection method is timer or query_metric then add to query_metrics
    for c in metric_index.collection_index():
        for m in metric_index.metrics_per_collection(c):
            if c == "timer" or c == "query_metric":
                t = metric_index.metric_type(m)
                if t == "uint64_t":
                    t = "atomic<idx_t>"
                elif t == "double":
                    t = "Profiler"
                query_metric_types.append((m, t, metric_index.metric_description(m)))

    # Move query_name to the front
    query_name_items = [item for item in query_metric_types if item[0] == "QUERY_NAME"]
    other_items = [item for item in query_metric_types if item[0] != "QUERY_NAME"]
    query_metric_types = query_name_items + other_items

    hpp_f.write_indented(0, "//! Top level query metrics.")
    hpp_f.write_indented(0, "struct QueryMetrics {")

    query_metric_constructor = "QueryMetrics() : "
    for m, t, d in query_metric_types:
        if t == "atomic<idx_t>":
            query_metric_constructor += f"{m.lower()}(0), "
        elif t == "string":
            query_metric_constructor += f"{m.lower()}(\"\"), "
    # remove trailing comma
    query_metric_constructor = query_metric_constructor[:-2]
    query_metric_constructor += " {};\n"
    hpp_f.write_indented(1, query_metric_constructor)

    hpp_f.write_indented(1, "//! Reset the query metrics")
    hpp_f.write_indented(1, "void Reset() {")
    for m, t, d in query_metric_types:
        if t == "atomic<idx_t>":
            hpp_f.write_indented(2, f"{m.lower()} = 0;")
        elif t == "string":
            hpp_f.write_indented(2, f"{m.lower()} = \"\";")
        elif t == "Profiler":
            hpp_f.write_indented(2, f"{m.lower()}.Reset();")
    hpp_f.write_indented(1, "}\n")

    _write_query_metric_functions(
        hpp_f, query_metric_types, "StartTimer(const MetricType type)", "Profiler", ".Start()"
    )
    _write_query_metric_functions(hpp_f, query_metric_types, "EndTimer(const MetricType type)", "Profiler", ".End()")
    _write_query_metric_functions(
        hpp_f,
        query_metric_types,
        "AddToCounter(const MetricType type, const idx_t amount)",
        "atomic<idx_t>",
        " += amount",
    )

    hpp_f.write_indented(1, "ProfilingInfo query_global_info;\n")

    for m, t, d in query_metric_types:
        # if the type is a Profiler (ie timer) replace "Time spent" with "The timer for"
        if t == "Profiler":
            if d.find("Time spent") == -1:
                raise Exception(
                    f"Could not find 'Time spent' in metric description for {m}, description should match 'Time spent <description>'"
                )
            d = d.replace("Time spent", "The timer for")
        hpp_f.write_indented(1, f"//! {d}")
        hpp_f.write_indented(1, f"{t} {m.lower()};")

    hpp_f.write_indented(0, "};")


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
