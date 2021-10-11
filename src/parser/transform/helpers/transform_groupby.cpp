#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression_map.hpp"

namespace duckdb {

bool Transformer::TransformGroupBy(duckdb_libpgquery::PGList *group, GroupByNode &result) {
	if (!group) {
		return false;
	}
	expression_map_t<idx_t> map;
	for (auto node = group->head; node != nullptr; node = node->next) {
		auto n = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
		if (n->type == duckdb_libpgquery::T_PGGroupingSet) {
			auto grouping_set = (duckdb_libpgquery::PGGroupingSet *) n;
			throw ParserException("FIXME");
		} else {
			result.group_expressions.push_back(TransformExpression(n, 0));
		}
	}
	return true;
}

} // namespace duckdb
