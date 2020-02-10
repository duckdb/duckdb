#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundOperatorExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	return result;
}

void ExpressionExecutor::Execute(BoundOperatorExpression &expr, ExpressionState *state, Vector &result) {
	// special handling for special snowflake 'IN'
	// IN has n children
	if (expr.type == ExpressionType::COMPARE_IN || expr.type == ExpressionType::COMPARE_NOT_IN) {
		if (expr.children.size() < 2) {
			throw Exception("IN needs at least two children");
		}
		Vector left(expr.children[0]->return_type);
		// eval left side
		Execute(*expr.children[0], state->child_states[0].get(), left);

		// init result to false
		Vector intermediate;
		intermediate.Initialize(TypeId::BOOL);
		intermediate.SetCount(left.size());
		intermediate.SetSelVector(left.sel_vector());
		VectorOperations::Set(intermediate, Value(false));

		// in rhs is a list of constants
		// for every child, OR the result of the comparision with the left
		// to get the overall result.
		for (index_t child = 1; child < expr.children.size(); child++) {
			Vector vector_to_check(expr.children[child]->return_type);
			Vector comp_res(TypeId::BOOL);

			Execute(*expr.children[child], state->child_states[child].get(), vector_to_check);
			VectorOperations::Equals(left, vector_to_check, comp_res);

			if (child == 1) {
				// first child: move to result
				intermediate.Reference(comp_res);
			} else {
				// otherwise OR together
				Vector new_result(TypeId::BOOL, true, false);
				VectorOperations::Or(intermediate, comp_res, new_result);
				intermediate.Reference(new_result);
			}
		}
		if (expr.type == ExpressionType::COMPARE_NOT_IN) {
			// NOT IN: invert result
			VectorOperations::Not(intermediate, result);
		} else {
			// directly use the result
			result.Reference(intermediate);
		}
	} else if (expr.children.size() == 1) {
		Vector child(expr.children[0]->return_type);
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
