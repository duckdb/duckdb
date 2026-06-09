#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/json_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/html_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/graphviz_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/yaml_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/mermaid_tree_renderer.hpp"
#include "duckdb/common/enums/explain_format.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Profiler output (base implementations)
//===--------------------------------------------------------------------===//
string TreeRenderer::RenderProfiler(const QueryProfiler &profiler) {
	// by default, render the profiling node tree using this renderer (covers HTML/GraphViz/Mermaid)
	return profiler.RenderProfilingNodeTree(*this);
}

string TreeRenderer::RenderProfilerDisabled() {
	return "Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!";
}

//===--------------------------------------------------------------------===//
// Explain format registry
//===--------------------------------------------------------------------===//
// Single source of truth mapping an explain format name to the TreeRenderer that renders it. The EXPLAIN parser
// (ExplainFormat::FromString), the query profiler, and the TreeRenderer factory all resolve formats through this
// table.
template <class T>
static unique_ptr<TreeRenderer> MakeRenderer() {
	return make_uniq<T>();
}

struct ExplainFormatEntry {
	const char *name;
	unique_ptr<TreeRenderer> (*create_renderer)();
};

static const ExplainFormatEntry EXPLAIN_FORMATS[] = {
    {"default", MakeRenderer<TextTreeRenderer>},      {"text", MakeRenderer<TextTreeRenderer>},
    {"json", MakeRenderer<JSONTreeRenderer>},         {"html", MakeRenderer<HTMLTreeRenderer>},
    {"graphviz", MakeRenderer<GRAPHVIZTreeRenderer>}, {"yaml", MakeRenderer<YAMLTreeRenderer>},
    {"mermaid", MakeRenderer<MermaidTreeRenderer>},
};

//! Look up the registry entry for a format name, throwing InvalidInputException (listing valid names) when unknown.
static const ExplainFormatEntry &LookupExplainFormat(const string &name) {
	auto lower = StringUtil::Lower(name);
	for (auto &entry : EXPLAIN_FORMATS) {
		if (lower == entry.name) {
			return entry;
		}
	}
	vector<string> options;
	for (auto &entry : EXPLAIN_FORMATS) {
		options.push_back(entry.name);
	}
	throw InvalidInputException("\"%s\" is not a valid FORMAT argument, valid options are: %s", name,
	                            StringUtil::Join(options, ", "));
}

ExplainFormat ExplainFormat::FromString(const string &name) {
	// return the canonical (lowercase) name for the matched entry
	return ExplainFormat(LookupExplainFormat(name).name);
}

unique_ptr<TreeRenderer> TreeRenderer::CreateRenderer(const string &name) {
	return LookupExplainFormat(name).create_renderer();
}

unique_ptr<TreeRenderer> TreeRenderer::CreateRenderer(const ExplainFormat &format) {
	return CreateRenderer(format.ToString());
}

} // namespace duckdb
