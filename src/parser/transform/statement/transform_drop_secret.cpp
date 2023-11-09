#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"

namespace duckdb {

unique_ptr<PragmaStatement> Transformer::TransformDropSecret(duckdb_libpgquery::PGDropSecretStmt &stmt) {
	auto result = make_uniq<PragmaStatement>();

	if (stmt.secret_name) {
		result->info->name = "duckdb_drop_secret";
	}
	result->info->parameters.push_back(stmt.secret_name);
	result->info->parameters.push_back(Value(stmt.missing_ok).DefaultCastAs(LogicalType::BOOLEAN));

	return result;
}

} // namespace duckdb
