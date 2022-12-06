#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/set_statement.hpp"

namespace duckdb {

unique_ptr<SetStatement> Transformer::TransformUse(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGUseStmt *>(node);
	return make_unique<SetStatement>("database", stmt->name, SetScope::AUTOMATIC);
}

} // namespace duckdb
