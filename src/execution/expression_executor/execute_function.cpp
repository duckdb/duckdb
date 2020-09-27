#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundFunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(BoundFunctionExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	DataChunk arguments;
	if (state->types.size() > 0) {
		arguments.InitializeEmpty(state->types);
		arguments.Reference(state->intermediate_chunk);
		for (idx_t i = 0; i < expr.children.size(); i++) {
			assert(state->types[i] == expr.children[i]->return_type);
			Execute(*expr.children[i], state->child_states[i].get(), sel, count, arguments.data[i]);
#ifdef DEBUG
			if (expr.children[i]->return_type.id() == LogicalTypeId::VARCHAR) {
				arguments.data[i].UTFVerify(count);
			}
#endif
		}
		arguments.Verify();
	}
	arguments.SetCardinality(count);
	expr.function.function(arguments, *state, result);

	if (result.type != expr.return_type) {
		throw TypeMismatchException(expr.return_type, result.type,
		                            "expected function to return the former "
		                            "but the function returned the latter");
	}
}

} // namespace duckdb
