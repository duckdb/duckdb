#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

struct FunctionState : public ExpressionState {
	FunctionState(Expression &expr, ExpressionExecutorState &root)
	    : ExpressionState(expr, root) {
		auto &func = (BoundFunctionExpression&) expr;
		for(auto &child : func.children) {
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

void ExpressionExecutor::Execute(BoundFunctionExpression &expr, ExpressionState *state_, Vector &result) {
	auto state = (FunctionState *)state_;
	DataChunk arguments;
	if (state->child_types.size() > 0) {
		arguments.InitializeEmpty(state->child_types);
		for (index_t i = 0; i < expr.children.size(); i++) {
			assert(state->child_types[i] == expr.children[i]->return_type);
			arguments.data[i].Initialize(state->child_types[i]);
			Execute(*expr.children[i], state->child_states[i].get(), arguments.data[i]);
		}
		arguments.sel_vector = arguments.data[0].sel_vector();
		arguments.Verify();
	}
	expr.function.function(arguments, *state, result);
	if (result.type != expr.return_type) {
		throw TypeMismatchException(expr.return_type, result.type,
		                            "expected function to return the former "
		                            "but the function returned the latter");
	}
}
