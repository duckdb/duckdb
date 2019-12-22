#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionState::AddIntermediates(vector<Expression *> expressions) {
	vector<TypeId> types;
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
		child_states.push_back(ExpressionExecutor::InitializeState(*expr, root));
	}
	if (types.size() > 0) {
		arguments.Initialize(types);
	}
}
