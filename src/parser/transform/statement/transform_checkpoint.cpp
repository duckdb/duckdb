#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformCheckpoint(duckdb_libpgquery::PGNode *node) {
	auto checkpoint = (duckdb_libpgquery::PGCheckPointStmt *)node;

	vector<unique_ptr<ParsedExpression>> children;
	// transform into "CALL checkpoint()" or "CALL force_checkpoint()"
	auto checkpoint_name = checkpoint->force ? "force_checkpoint" : "checkpoint";
	auto result = make_unique<CallStatement>();
	auto function = make_unique<FunctionExpression>(checkpoint_name, std::move(children));
	if (checkpoint->name) {
		function->children.push_back(make_unique<ConstantExpression>(Value(checkpoint->name)));
	}
	result->function = std::move(function);
	return std::move(result);
}

} // namespace duckdb
