#include <string>
#include <utility>

#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformCheckpoint(duckdb_libpgquery::PGCheckPointStmt &stmt) {
	vector<unique_ptr<ParsedExpression>> children;
	// transform into "CALL checkpoint()" or "CALL force_checkpoint()"
	auto checkpoint_name = stmt.force ? "force_checkpoint" : "checkpoint";
	auto result = make_uniq<CallStatement>();
	auto function = make_uniq<FunctionExpression>(checkpoint_name, std::move(children));
	function->catalog = SYSTEM_CATALOG;
	function->schema = DEFAULT_SCHEMA;
	if (stmt.name) {
		function->children.push_back(make_uniq<ConstantExpression>(Value(stmt.name)));
	}
	result->function = std::move(function);
	return std::move(result);
}

} // namespace duckdb
