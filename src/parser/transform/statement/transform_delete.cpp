#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformDelete(duckdb_libpgquery::PGDeleteStmt &stmt) {
	auto result = make_uniq<DeleteStatement>();
	vector<unique_ptr<CTENode>> materialized_ctes;
	CommonTableExpressionMap cte_map;
	if (stmt.withClause) {
		TransformCTE(*PGPointerCast<duckdb_libpgquery::PGWithClause>(stmt.withClause), cte_map, materialized_ctes);
		result->cte_map = cte_map.Copy();
	}

	result->condition = TransformExpression(stmt.whereClause);
	result->table = TransformRangeVar(*stmt.relation);
	if (result->table->type != TableReferenceType::BASE_TABLE) {
		throw InvalidInputException("Can only delete from base tables!");
	}
	if (stmt.usingClause) {
		for (auto n = stmt.usingClause->head; n != nullptr; n = n->next) {
			auto target = PGPointerCast<duckdb_libpgquery::PGNode>(n->data.ptr_value);
			auto using_entry = TransformTableRefNode(*target);
			result->using_clauses.push_back(std::move(using_entry));
		}
	}

	if (stmt.returningList) {
		TransformExpressionList(*stmt.returningList, result->returning_list);
	}

	// Handle materialized CTEs
	auto cte_result = Transformer::TransformMaterializedCTEStatement(std::move(result), materialized_ctes, cte_map);

	return cte_result;
}

} // namespace duckdb
