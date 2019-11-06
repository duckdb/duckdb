#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundOperatorExpression &expr, Vector &result) {
	// special handling for special snowflake 'IN'
	// IN has n children
	if (expr.type == ExpressionType::COMPARE_IN || expr.type == ExpressionType::COMPARE_NOT_IN) {
		if (expr.children.size() < 2) {
			throw Exception("IN needs at least two children");
		}
		Vector left;
		// eval left side
		Execute(*expr.children[0], left);

		// init result to false
		Vector intermediate;
		intermediate.Initialize(TypeId::BOOLEAN);
		intermediate.count = left.count;
		intermediate.sel_vector = left.sel_vector;
		VectorOperations::Set(intermediate, Value(false));

		assert(expr.children[1]->type != ExpressionType::SUBQUERY);
		// in rhs is a list of constants
		// for every child, OR the result of the comparision with the left
		// to get the overall result.
		for (index_t child = 1; child < expr.children.size(); child++) {
			Vector comp_res(TypeId::BOOLEAN, true, false);

			Vector vector_to_check;
			Execute(*expr.children[child], vector_to_check);
			VectorOperations::Equals(left, vector_to_check, comp_res);

			if (child == 1) {
				// first child: move to result
				comp_res.Move(intermediate);
			} else {
				// otherwise OR together
				Vector new_result(TypeId::BOOLEAN, true, false);
				VectorOperations::Or(intermediate, comp_res, new_result);
				new_result.Move(intermediate);
			}
		}
		if (expr.type == ExpressionType::COMPARE_NOT_IN) {
			// NOT IN: invert result
			result.Initialize(TypeId::BOOLEAN);
			VectorOperations::Not(intermediate, result);
		} else {
			// directly use the result
			intermediate.Move(result);
		}
	} else if (expr.children.size() == 1) {
		Vector child;
		Execute(*expr.children[0], child);
		switch (expr.type) {
		case ExpressionType::OPERATOR_NOT: {
			result.Initialize(TypeId::BOOLEAN);
			VectorOperations::Not(child, result);
			break;
		}
		case ExpressionType::OPERATOR_IS_NULL: {
			result.Initialize(TypeId::BOOLEAN);
			VectorOperations::IsNull(child, result);
			break;
		}
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			result.Initialize(TypeId::BOOLEAN);
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
