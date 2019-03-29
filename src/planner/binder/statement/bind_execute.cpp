#include "main/client_context.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/statement/execute_statement.hpp"
#include "planner/binder.hpp"
#include "planner/statement/bound_execute_statement.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression_binder/constant_binder.hpp"

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
	if (stmt.values.size() != result->prep->value_map.size()) {
		throw BinderException("Parameter/argument count mismatch");
	}
	// bind the values
	for (auto &expr : stmt.values) {
		ConstantBinder binder(*this, context, "EXECUTE statement");
		auto bound_expr = binder.Bind(expr);
		
		Value value = ExpressionExecutor::EvaluateScalar(*bound_expr);
		result->values.push_back(value);
	}
	return move(result);
}
