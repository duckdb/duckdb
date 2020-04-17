#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionState::AddChild(Expression *expr) {
	child_states.push_back(ExpressionExecutor::InitializeState(*expr, root));
}
