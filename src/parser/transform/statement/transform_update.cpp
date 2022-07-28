#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<UpdateStatement> Transformer::TransformUpdate(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGUpdateStmt *>(node);
	D_ASSERT(stmt);

	auto result = make_unique<UpdateStatement>();
	if (stmt->withClause) {
		TransformCTE(reinterpret_cast<duckdb_libpgquery::PGWithClause *>(stmt->withClause), result->cte_map);
	}

	result->table = TransformRangeVar(stmt->relation);
	if (stmt->fromClause) {
		result->from_table = TransformFrom(stmt->fromClause);
	}

	auto root = stmt->targetList;
	for (auto cell = root->head; cell != nullptr; cell = cell->next) {
		auto target = (duckdb_libpgquery::PGResTarget *)(cell->data.ptr_value);
		result->columns.emplace_back(target->name);
		result->expressions.push_back(TransformExpression(target->val));
	}

	// Grab and transform the returning columns from the parser.
	if (stmt->returningList) {
		Transformer::TransformExpressionList(*(stmt->returningList), result->returning_list);
	}

	result->condition = TransformExpression(stmt->whereClause);
	return result;
}

} // namespace duckdb
