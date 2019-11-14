#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/statement/bound_execute_statement.hpp"

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
	index_t param_idx = 1;
	for (auto &expr : stmt.values) {
		auto it = result->prep->value_map.find(param_idx);
		if (it == result->prep->value_map.end()) {
			throw Exception("Could not find parameter with this index");
		}
		auto &target = it->second;

		ConstantBinder binder(*this, context, "EXECUTE statement");
		binder.target_type = target.target_type;
		auto bound_expr = binder.Bind(expr);

		Value value = ExpressionExecutor::EvaluateScalar(*bound_expr);
		assert(target.value);
		*target.value = value;
		param_idx++;
	}
	return move(result);
}
