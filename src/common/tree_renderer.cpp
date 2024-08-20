#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/json_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/html_tree_renderer.hpp"
#include "duckdb/common/tree_renderer/graphviz_tree_renderer.hpp"

#include <sstream>

namespace duckdb {

unique_ptr<TreeRenderer> TreeRenderer::CreateRenderer(ExplainFormat format) {
	switch (format) {
	case ExplainFormat::DEFAULT:
	case ExplainFormat::TEXT:
		return make_uniq<TextTreeRenderer>();
	case ExplainFormat::JSON:
		return make_uniq<JSONTreeRenderer>();
	case ExplainFormat::HTML:
		return make_uniq<HTMLTreeRenderer>();
	case ExplainFormat::GRAPHVIZ:
		return make_uniq<GRAPHVIZTreeRenderer>();
	default:
		throw NotImplementedException("ExplainFormat %s not implemented", EnumUtil::ToString(format));
	}
}

} // namespace duckdb
