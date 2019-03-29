#include "main/client_context.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/statement/execute_statement.hpp"
#include "planner/binder.hpp"
#include "planner/statement/bound_execute_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(ExecuteStatement &stmt) {
	auto result = make_unique<BoundExecuteStatement>();
	// bind the prepared statement
	result->prep =
	    (PreparedStatementCatalogEntry *)context.prepared_statements->GetEntry(context.ActiveTransaction(), stmt.name);
	if (!result->prep || result->prep->deleted) {
		throw BinderException("Could not find prepared statement with that name");
	}
	// set parameters
	if (stmt.values.size() != result->prep->parameter_expression_map.size()) {
		throw BinderException("Parameter/argument count mismatch");
	}
	// bind the values
	for (auto &expr : stmt.values) {
		if (expr->type != ExpressionType::VALUE_CONSTANT) {
			// not a constant!
			throw BinderException("Can only execute with scalar parameters!");
		}
		auto &const_expr = (ConstantExpression &)*expr;
		result->values.push_back(const_expr.value);
	}
	return move(result);
}
