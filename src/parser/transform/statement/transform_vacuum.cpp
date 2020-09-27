#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<VacuumStatement> Transformer::TransformVacuum(PGNode *node) {
	auto stmt = reinterpret_cast<PGVacuumStmt *>(node);
	assert(stmt);
	(void)stmt;
	auto result = make_unique<VacuumStatement>();
	return result;
}

} // namespace duckdb
