#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
unique_ptr<SQLStatement> Transformer::TransformDropPropertyGraph(duckdb_libpgquery::PGNode *node) {
	auto stmt = (duckdb_libpgquery::PGDropPropertyGraphStmt *)(node);
	auto result = make_uniq<DropStatement>();
	auto &info = *result->info.get();
	D_ASSERT(stmt);
	auto pg_tableref = TransformQualifiedName(stmt->name);

	info.name = pg_tableref.name;
	info.type = CatalogType::TABLE_ENTRY;
	return result;
}

} // namespace duckdb
