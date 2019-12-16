#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/statement/bound_execute_statement.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"

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
	vector<Value> bind_values;
	for (index_t i = 0; i < stmt.values.size(); i++) {
		ConstantBinder binder(*this, context, "EXECUTE statement");
		binder.target_type = result->prep->prepared->GetType(i + 1);
		auto bound_expr = binder.Bind(stmt.values[i]);

		Value value = ExpressionExecutor::EvaluateScalar(*bound_expr);
		bind_values.push_back(move(value));
	}
	result->prep->prepared->Bind(move(bind_values));
	return move(result);
}
