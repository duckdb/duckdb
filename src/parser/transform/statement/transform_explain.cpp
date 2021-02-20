#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ExplainStatement> Transformer::TransformExplain(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGExplainStmt *>(node);
	D_ASSERT(stmt);
	return make_unique<ExplainStatement>(TransformStatement(stmt->query));
}

} // namespace duckdb
