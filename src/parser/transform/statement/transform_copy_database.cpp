#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<CopyDatabaseStatement> Transformer::TransformCopyDatabase(duckdb_libpgquery::PGCopyDatabaseStmt &stmt) {
	auto result = make_uniq<CopyDatabaseStatement>(stmt.from_database, stmt.to_database);
	return result;
}

} // namespace duckdb
