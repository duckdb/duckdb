#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<VacuumStatement> Transformer::TransformVacuum(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGVacuumStmt *>(node);
	D_ASSERT(stmt);
	(void)stmt;
	auto result = make_unique<VacuumStatement>();
	return result;
}

} // namespace duckdb
