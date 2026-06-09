#include "duckdb/main/profiler_printer.hpp"

#include "duckdb/main/query_profiler.hpp"

#include "yyjson.hpp"
#include "yyjson_utils.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

ProfilerPrinter::ProfilerPrinter(const QueryProfiler &profiler_p) : profiler(profiler_p) {
}

ProfilerPrinter::~ProfilerPrinter() {
}

//===--------------------------------------------------------------------===//
// Query Tree
//===--------------------------------------------------------------------===//
QueryTreeProfilerPrinter::QueryTreeProfilerPrinter(const QueryProfiler &profiler) : ProfilerPrinter(profiler) {
}

string QueryTreeProfilerPrinter::ToString() const {
	return profiler.QueryTreeToString();
}

string QueryTreeProfilerPrinter::RenderDisabledMessage() const {
	return "Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!";
}

//===--------------------------------------------------------------------===//
// Query Tree (Optimizer)
//===--------------------------------------------------------------------===//
QueryTreeOptimizerProfilerPrinter::QueryTreeOptimizerProfilerPrinter(const QueryProfiler &profiler)
    : QueryTreeProfilerPrinter(profiler) {
}

bool QueryTreeOptimizerProfilerPrinter::PrintOptimizerOutput() const {
	return true;
}

//===--------------------------------------------------------------------===//
// JSON
//===--------------------------------------------------------------------===//
JSONProfilerPrinter::JSONProfilerPrinter(const QueryProfiler &profiler) : ProfilerPrinter(profiler) {
}

string JSONProfilerPrinter::ToString() const {
	return profiler.ToJSON();
}

string JSONProfilerPrinter::RenderDisabledMessage() const {
	ConvertedJSONHolder json_holder;
	json_holder.doc = yyjson_mut_doc_new(nullptr);
	auto result_obj = yyjson_mut_obj(json_holder.doc);
	yyjson_mut_doc_set_root(json_holder.doc, result_obj);

	yyjson_mut_obj_add_str(json_holder.doc, result_obj, "result", "disabled");
	json_holder.stringified_json = yyjson_mut_val_write_opts(
	    result_obj, YYJSON_WRITE_ALLOW_INF_AND_NAN | YYJSON_WRITE_PRETTY, nullptr, nullptr, nullptr);
	if (!json_holder.stringified_json) {
		throw InternalException("The plan could not be rendered as JSON, yyjson failed");
	}
	return string(json_holder.stringified_json);
}

//===--------------------------------------------------------------------===//
// No Output
//===--------------------------------------------------------------------===//
NoOutputProfilerPrinter::NoOutputProfilerPrinter(const QueryProfiler &profiler) : ProfilerPrinter(profiler) {
}

string NoOutputProfilerPrinter::ToString() const {
	return "";
}

string NoOutputProfilerPrinter::RenderDisabledMessage() const {
	return "";
}

bool NoOutputProfilerPrinter::EmitsOutput() const {
	return false;
}

//===--------------------------------------------------------------------===//
// Tree Renderer (HTML / GraphViz / Mermaid)
//===--------------------------------------------------------------------===//
TreeRendererProfilerPrinter::TreeRendererProfilerPrinter(const QueryProfiler &profiler, ExplainFormat format_p)
    : ProfilerPrinter(profiler), format(format_p) {
}

string TreeRendererProfilerPrinter::ToString() const {
	return profiler.RenderTree(format);
}

HTMLProfilerPrinter::HTMLProfilerPrinter(const QueryProfiler &profiler)
    : TreeRendererProfilerPrinter(profiler, ExplainFormat::HTML) {
}

string HTMLProfilerPrinter::RenderDisabledMessage() const {
	return R"(
				<!DOCTYPE html>
                <html lang="en"><head/><body>
                  Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!
                </body></html>
			)";
}

GraphVizProfilerPrinter::GraphVizProfilerPrinter(const QueryProfiler &profiler)
    : TreeRendererProfilerPrinter(profiler, ExplainFormat::GRAPHVIZ) {
}

string GraphVizProfilerPrinter::RenderDisabledMessage() const {
	return R"(
				digraph G {
				    node [shape=box, style=rounded, fontname="Courier New", fontsize=10];
				    node_0_0 [label="Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!"];
				}
			)";
}

MermaidProfilerPrinter::MermaidProfilerPrinter(const QueryProfiler &profiler)
    : TreeRendererProfilerPrinter(profiler, ExplainFormat::MERMAID) {
}

string MermaidProfilerPrinter::RenderDisabledMessage() const {
	return R"(flowchart TD
    node_0_0["`**DISABLED**
Query profiling is disabled.
Use 'PRAGMA enable_profiling;' to enable profiling!`"]
)";
}

} // namespace duckdb
