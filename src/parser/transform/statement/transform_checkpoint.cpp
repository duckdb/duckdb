#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformCheckpoint(duckdb_libpgquery::PGCheckPointStmt &stmt) {
	vector<unique_ptr<ParsedExpression>> children;
	// transform into "CALL checkpoint()" or "CALL force_checkpoint()"
	auto checkpoint_name = stmt.force ? "force_checkpoint" : "checkpoint";
	auto result = make_uniq<CallStatement>();
	auto function = make_uniq<FunctionExpression>(checkpoint_name, std::move(children));
	if (stmt.name) {
		function->children.push_back(make_uniq<ConstantExpression>(Value(stmt.name)));
	}
	result->function = std::move(function);
	return std::move(result);
}

} // namespace duckdb
