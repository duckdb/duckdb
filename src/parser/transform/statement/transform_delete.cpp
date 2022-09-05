#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/tableref/crossproductref.hpp"

namespace duckdb {

unique_ptr<DeleteStatement> Transformer::TransformDelete(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGDeleteStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<DeleteStatement>();
	if (stmt->withClause) {
		TransformCTE(reinterpret_cast<duckdb_libpgquery::PGWithClause *>(stmt->withClause), result->cte_map);
	}

	result->condition = TransformExpression(stmt->whereClause);
	result->table = TransformRangeVar(stmt->relation);
	if (result->table->type != TableReferenceType::BASE_TABLE) {
		throw Exception("Can only delete from base tables!");
	}
	if (stmt->usingClause) {
		for (auto n = stmt->usingClause->head; n != nullptr; n = n->next) {
			auto target = reinterpret_cast<duckdb_libpgquery::PGNode *>(n->data.ptr_value);
			auto using_entry = TransformTableRefNode(target);
			result->using_clauses.push_back(move(using_entry));
		}
	}

	if (stmt->returningList) {
		Transformer::TransformExpressionList(*(stmt->returningList), result->returning_list);
	}
	return result;
}

} // namespace duckdb
