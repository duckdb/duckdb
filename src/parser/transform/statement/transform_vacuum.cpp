#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformVacuum(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGVacuumStmt *>(node);
	D_ASSERT(stmt);

	auto result = make_unique<VacuumStatement>(stmt->options);

	if (stmt->relation) {
		result->info->ref = TransformRangeVar(stmt->relation);
		result->info->has_table = true;
	}

	if (stmt->va_cols) {
		D_ASSERT(result->info->has_table);
		for (auto col_node = stmt->va_cols->head; col_node != nullptr; col_node = col_node->next) {
			result->info->columns.emplace_back(
			    reinterpret_cast<duckdb_libpgquery::PGValue *>(col_node->data.ptr_value)->val.str);
		}
	}

	return move(result);
}

} // namespace duckdb
