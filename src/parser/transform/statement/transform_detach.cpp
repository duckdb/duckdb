#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/detach_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<DetachStatement> Transformer::TransformDetach(duckdb_libpgquery::PGDetachStmt &stmt) {
	auto result = make_uniq<DetachStatement>();
	auto info = make_uniq<DetachInfo>();
	info->name = stmt.db_name;
	info->if_not_found = TransformOnEntryNotFound(stmt.missing_ok);

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
