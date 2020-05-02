#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

struct FunctionState : public ExpressionState {
	FunctionState(Expression &expr, ExpressionExecutorState &root) : ExpressionState(expr, root) {
		auto &func = (BoundFunctionExpression &)expr;
		for (auto &child : func.children) {
			child_types.push_back(child->return_type);
		}
	}

	vector<TypeId> child_types;
};

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundFunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<FunctionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	return move(result);
}

void ExpressionExecutor::Execute(BoundFunctionExpression &expr, ExpressionState *state_, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	auto state = (FunctionState *)state_;
	DataChunk arguments;
	arguments.SetCardinality(count);
	if (state->child_types.size() > 0) {
		arguments.Initialize(state->child_types);
		for (idx_t i = 0; i < expr.children.size(); i++) {
			assert(state->child_types[i] == expr.children[i]->return_type);
			Execute(*expr.children[i], state->child_states[i].get(), sel, count, arguments.data[i]);
#ifdef DEBUG
			if (expr.arguments[i].id == SQLTypeId::VARCHAR) {
				arguments.data[i].UTFVerify(count);
			}
#endif
		}
		arguments.Verify();
	}
	expr.function.function(arguments, *state, result);
	if (result.type != expr.return_type) {
		throw TypeMismatchException(expr.return_type, result.type,
		                            "expected function to return the former "
		                            "but the function returned the latter");
	}
}
