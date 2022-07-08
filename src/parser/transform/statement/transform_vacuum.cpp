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
	return move(result);
}

} // namespace duckdb
