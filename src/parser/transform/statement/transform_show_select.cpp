#include "duckdb/parser/statement/show_statement.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ShowStatement> Transformer::TransformShowSelect(duckdb_libpgquery::PGNode *node) {
	// we capture the select statement of SHOW
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGVariableShowSelectStmt *>(node);
	auto select_stmt = reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(stmt->stmt);

	auto result = make_unique<ShowStatement>();
	auto &info = *result->info;
	info.is_summary = stmt->is_summary;

	info.query = TransformSelectNode(select_stmt);

	return result;
}

} // namespace duckdb
