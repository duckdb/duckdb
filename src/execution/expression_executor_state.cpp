#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
using namespace std;

void ExpressionState::AddChild(Expression *expr) {
	types.push_back(expr->return_type);
	child_states.push_back(ExpressionExecutor::InitializeState(*expr, root));
}

void ExpressionState::Finalize() {
	if (types.size() > 0) {
		intermediate_chunk.Initialize(types);
	}
}

} // namespace duckdb
