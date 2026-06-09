#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/json_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/html_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/graphviz_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/yaml_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/mermaid_tree_renderer.hpp"
#include "duckdb/common/enums/explain_format.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Explain format registry
//===--------------------------------------------------------------------===//
// Single source of truth mapping an explain format name to its ExplainFormat enum value (kept for plan serialization)
// and to the TreeRenderer that renders it. The EXPLAIN parser, the query profiler, and the TreeRenderer factory all
// resolve formats through this table.
template <class T>
static unique_ptr<TreeRenderer> MakeRenderer() {
	return make_uniq<T>();
}

struct ExplainFormatType {
	const char *name;
	ExplainFormat format;
	unique_ptr<TreeRenderer> (*create_renderer)();
};

static const ExplainFormatType EXPLAIN_FORMATS[] = {
    {"default", ExplainFormat::DEFAULT, MakeRenderer<TextTreeRenderer>},
    {"text", ExplainFormat::TEXT, MakeRenderer<TextTreeRenderer>},
    {"json", ExplainFormat::JSON, MakeRenderer<JSONTreeRenderer>},
    {"html", ExplainFormat::HTML, MakeRenderer<HTMLTreeRenderer>},
    {"graphviz", ExplainFormat::GRAPHVIZ, MakeRenderer<GRAPHVIZTreeRenderer>},
    {"yaml", ExplainFormat::YAML, MakeRenderer<YAMLTreeRenderer>},
    {"mermaid", ExplainFormat::MERMAID, MakeRenderer<MermaidTreeRenderer>},
};

//! Look up the registry entry for a format name, throwing InvalidInputException (listing valid names) when unknown.
static const ExplainFormatType &LookupExplainFormat(const string &name) {
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

ExplainFormat ExplainFormatFromString(const string &name) {
	return LookupExplainFormat(name).format;
}

string ExplainFormatToString(ExplainFormat format) {
	for (auto &entry : EXPLAIN_FORMATS) {
		if (entry.format == format) {
			return entry.name;
		}
	}
	throw InternalException("No name registered for ExplainFormat %s", EnumUtil::ToString(format));
}

unique_ptr<TreeRenderer> TreeRenderer::CreateRenderer(const string &name) {
	return LookupExplainFormat(name).create_renderer();
}

unique_ptr<TreeRenderer> TreeRenderer::CreateRenderer(ExplainFormat format) {
	return CreateRenderer(ExplainFormatToString(format));
}

} // namespace duckdb
