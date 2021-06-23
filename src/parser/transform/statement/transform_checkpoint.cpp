#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformCheckpoint(duckdb_libpgquery::PGNode *node) {
	auto checkpoint = (duckdb_libpgquery::PGCheckPointStmt *)node;

	vector<unique_ptr<ParsedExpression>> children;
	// transform into "CALL checkpoint()" or "CALL force_checkpoint()"
	auto result = make_unique<CallStatement>();
	result->function =
	    make_unique<FunctionExpression>(checkpoint->force ? "force_checkpoint" : "checkpoint", move(children));
	return move(result);
}

} // namespace duckdb
