#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundOperatorExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	vector<Expression *> children;
	for (auto &child : expr.children) {
		children.push_back(child.get());
	}
	result->AddIntermediates(children);
	return result;
}

void ExpressionExecutor::Execute(BoundOperatorExpression &expr, ExpressionState *state, Vector &result) {
	// special handling for special snowflake 'IN'
	// IN has n children
	if (expr.type == ExpressionType::COMPARE_IN || expr.type == ExpressionType::COMPARE_NOT_IN) {
		if (expr.children.size() < 2) {
			throw Exception("IN needs at least two children");
		}
		auto &left = state->arguments.data[0];
		// eval left side
		Execute(*expr.children[0], state->child_states[0].get(), left);

		// init result to false
		Vector intermediate;
		intermediate.Initialize(TypeId::BOOL);
		intermediate.count = left.count;
		intermediate.sel_vector = left.sel_vector;
		VectorOperations::Set(intermediate, Value(false));

		// in rhs is a list of constants
		// for every child, OR the result of the comparision with the left
		// to get the overall result.
		for (index_t child = 1; child < expr.children.size(); child++) {
			Vector comp_res(TypeId::BOOL, true, false);

			auto &vector_to_check = state->arguments.data[child];
			Execute(*expr.children[child], state->child_states[child].get(), vector_to_check);
			VectorOperations::Equals(left, vector_to_check, comp_res);

			if (child == 1) {
				// first child: move to result
				comp_res.Move(intermediate);
			} else {
				// otherwise OR together
				Vector new_result(TypeId::BOOL, true, false);
				VectorOperations::Or(intermediate, comp_res, new_result);
				new_result.Move(intermediate);
			}
		}
		if (expr.type == ExpressionType::COMPARE_NOT_IN) {
			// NOT IN: invert result
			VectorOperations::Not(intermediate, result);
		} else {
			// directly use the result
			intermediate.Move(result);
		}
	} else if (expr.children.size() == 1) {
		auto &child = state->arguments.data[0];
		Execute(*expr.children[0], state->child_states[0].get(), child);
		switch (expr.type) {
		case ExpressionType::OPERATOR_NOT: {
			VectorOperations::Not(child, result);
			break;
		}
		case ExpressionType::OPERATOR_IS_NULL: {
			VectorOperations::IsNull(child, result);
			break;
		}
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			VectorOperations::IsNotNull(child, result);
			break;
		}
		default:
			throw NotImplementedException("Unsupported operator type with 1 child!");
		}
	} else {
		throw NotImplementedException("operator");
	}
}
