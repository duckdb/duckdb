#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<UpdateSetInfo> Transformer::TransformUpdateSetInfo(duckdb_libpgquery::PGList *target_list,
                                                              duckdb_libpgquery::PGNode *where_clause) {
	auto result = make_uniq<UpdateSetInfo>();

	auto root = target_list;
	for (auto cell = root->head; cell != nullptr; cell = cell->next) {
		auto target = PGPointerCast<duckdb_libpgquery::PGResTarget>(cell->data.ptr_value);
		result->columns.emplace_back(target->name);
		result->expressions.push_back(TransformExpression(target->val));
	}
	result->condition = TransformExpression(where_clause);
	return result;
}

unique_ptr<UpdateStatement> Transformer::TransformUpdate(duckdb_libpgquery::PGUpdateStmt &stmt) {
	auto result = make_uniq<UpdateStatement>();
	if (stmt.withClause) {
		TransformCTE(*PGPointerCast<duckdb_libpgquery::PGWithClause>(stmt.withClause), result->cte_map);
	}

	result->table = TransformRangeVar(*stmt.relation);
	if (stmt.fromClause) {
		result->from_table = TransformFrom(stmt.fromClause);
	}

	result->set_info = TransformUpdateSetInfo(stmt.targetList, stmt.whereClause);

	// Grab and transform the returning columns from the parser.
	if (stmt.returningList) {
		TransformExpressionList(*stmt.returningList, result->returning_list);
	}

	return result;
}

} // namespace duckdb
