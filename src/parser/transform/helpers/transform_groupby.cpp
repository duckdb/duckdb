#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

// FIXME: what is the difference between GroupBy and expression list?
bool Transformer::TransformGroupBy(duckdb_libpgquery::PGList *group, vector<unique_ptr<ParsedExpression>> &result) {
	if (!group) {
		return false;
	}

	for (auto node = group->head; node != nullptr; node = node->next) {
		auto n = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
		result.push_back(TransformExpression(n, 0));
	}
	return true;
}

} // namespace duckdb
