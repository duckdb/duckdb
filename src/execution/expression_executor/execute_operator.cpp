#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/bound_subquery_expression.hpp"
#include "parser/expression/operator_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Visit(OperatorExpression &expr) {
	// special handling for special snowflake 'IN'
	// IN has n children
	if (expr.type == ExpressionType::COMPARE_IN || expr.type == ExpressionType::COMPARE_NOT_IN) {
		if (expr.children.size() < 2) {
			throw Exception("IN needs at least two children");
		}
		Vector l;
		// eval left side
		Execute(expr.children[0]);
		vector.Move(l);

		// init result to false
		Vector result;
		result.Initialize(TypeId::BOOLEAN);
		result.count = l.count;
		result.sel_vector = l.sel_vector;
		VectorOperations::Set(result, Value(false));

		assert(expr.children[1]->type != ExpressionType::SUBQUERY);
		// in rhs is a list of constants
		// for every child, OR the result of the comparision with the left
		// to get the overall result.
		for (size_t child = 1; child < expr.children.size(); child++) {
			Vector comp_res(TypeId::BOOLEAN, true, false);
			Execute(expr.children[child]);
			VectorOperations::Equals(l, vector, comp_res);
			vector.Destroy();
			if (child == 1) {
				// first child: move to result
				comp_res.Move(result);
			} else {
				// otherwise OR together
				Vector new_result(TypeId::BOOLEAN, true, false);
				VectorOperations::Or(result, comp_res, new_result);
				new_result.Move(result);
			}
		}
		if (expr.type == ExpressionType::COMPARE_NOT_IN) {
			// invert result
			vector.Initialize(TypeId::BOOLEAN);
			VectorOperations::Not(result, vector);
		} else {
			// just move result
			result.Move(vector);
		}
		expr.stats.Verify(vector);
	} else if (expr.children.size() == 1) {
		Execute(expr.children[0]);
		switch (expr.type) {
		case ExpressionType::OPERATOR_NOT: {
			Vector l;

			vector.Move(l);
			vector.Initialize(l.type);

			VectorOperations::Not(l, vector);
			break;
		}
		case ExpressionType::OPERATOR_IS_NULL: {
			Vector l;
			vector.Move(l);
			vector.Initialize(TypeId::BOOLEAN);
			VectorOperations::IsNull(l, vector);
			break;
		}
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			Vector l;
			vector.Move(l);
			vector.Initialize(TypeId::BOOLEAN);
			VectorOperations::IsNotNull(l, vector);
			break;
		}
		default:
			throw NotImplementedException("Unsupported operator type with 1 child!");
		}
	} else if (expr.children.size() == 2) {
		Vector l, r;
		Execute(expr.children[0]);
		vector.Move(l);

		Execute(expr.children[1]);
		vector.Move(r);

		vector.Initialize(l.type);

		switch (expr.type) {
		case ExpressionType::OPERATOR_ADD:
			VectorOperations::Add(l, r, vector);
			break;
		case ExpressionType::OPERATOR_SUBTRACT:
			VectorOperations::Subtract(l, r, vector);
			break;
		case ExpressionType::OPERATOR_MULTIPLY:
			VectorOperations::Multiply(l, r, vector);
			break;
		case ExpressionType::OPERATOR_DIVIDE:
			VectorOperations::Divide(l, r, vector);
			break;
		case ExpressionType::OPERATOR_MOD:
			VectorOperations::Modulo(l, r, vector);
			break;
		default:
			throw NotImplementedException("Unsupported operator type with 2 children!");
		}
	} else {
		throw NotImplementedException("operator");
	}
	Verify(expr);
}
