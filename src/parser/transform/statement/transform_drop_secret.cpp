#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

unique_ptr<PragmaStatement> Transformer::TransformDropSecret(duckdb_libpgquery::PGDropSecretStmt &stmt) {
	auto result = make_uniq<PragmaStatement>();

	if (stmt.secret_name) {
		result->info->name = "duckdb_drop_secret";
	}
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value(stmt.secret_name)));
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value(stmt.missing_ok).DefaultCastAs(LogicalType::BOOLEAN)));

	return result;
}

} // namespace duckdb
