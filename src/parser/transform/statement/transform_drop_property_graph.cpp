#include "duckdb/parser/parsed_data/drop_property_graph_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
unique_ptr<SQLStatement> Transformer::TransformDropPropertyGraph(duckdb_libpgquery::PGDropPropertyGraphStmt &stmt) {
	auto pg_tableref = TransformQualifiedName(*stmt.name);
	auto drop_pg_info = make_uniq<DropPropertyGraphInfo>(pg_tableref.name, stmt.missing_ok);
	auto result = make_uniq<DropStatement>();
	result->info = std::move(drop_pg_info);
	return std::move(result);
}

} // namespace duckdb
