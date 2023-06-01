#include "duckdb/parser/statement/show_statement.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ShowStatement> Transformer::TransformShowSelect(duckdb_libpgquery::PGVariableShowSelectStmt &stmt) {
	// we capture the select statement of SHOW
	auto select_stmt = PGPointerCast<duckdb_libpgquery::PGSelectStmt>(stmt.stmt);

	auto result = make_uniq<ShowStatement>();
	auto &info = *result->info;
	info.is_summary = stmt.is_summary;

	info.query = TransformSelectNode(*select_stmt);

	return result;
}

} // namespace duckdb
