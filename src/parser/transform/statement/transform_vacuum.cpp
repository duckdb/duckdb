#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformVacuum(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGVacuumStmt *>(node);
	D_ASSERT(stmt);

	auto result = make_unique<VacuumStatement>();
	if (stmt->relation) {
		result->info->ref = TransformRangeVar(stmt->relation);
	}

	vector<unique_ptr<ParsedExpression>> columns;
	if (stmt->va_cols) {
		D_ASSERT(stmt->relation);
		TransformExpressionList(*stmt->va_cols, columns);
	}
	for (auto &col : columns) {
		// We can just assert these rather than throwing an error, because the parser should guarantee them
		D_ASSERT(col->GetExpressionType() == ExpressionType::COLUMN_REF);
		auto &colref = (ColumnRefExpression &)col;
		D_ASSERT(colref.column_names.size() == 1);
		result->info->columns.push_back(colref.column_names[0]);
	}

	return move(result);
}

} // namespace duckdb
