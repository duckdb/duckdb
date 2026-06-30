#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/statement/refresh_feature_statement.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundStatement Binder::Bind(RefreshFeatureStatement &stmt) {
	CallStatement call_statement;
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(Value(stmt.feature_name)));
	call_statement.function = make_uniq<FunctionExpression>("refresh_feature", std::move(args));
	return Bind(call_statement);
}

} // namespace duckdb