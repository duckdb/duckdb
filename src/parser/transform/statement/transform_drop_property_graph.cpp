#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
unique_ptr<SQLStatement> Transformer::TransformDropPropertyGraph(duckdb_libpgquery::PGDropPropertyGraphStmt &stmt) {
	auto result = make_uniq<DropStatement>();
	auto &info = *result->info.get();
	auto pg_tableref = TransformQualifiedName(*stmt.name);

	info.name = pg_tableref.name;
	info.type = CatalogType::TABLE_ENTRY;
	return std::move(result);
}

} // namespace duckdb
