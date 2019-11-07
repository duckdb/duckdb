#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundComparisonExpression &expr, Vector &result) {
	Vector left, right;
	Execute(*expr.left, left);
	Execute(*expr.right, right);

	result.Initialize(TypeId::BOOLEAN);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(left, right, result);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(left, right, result);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(left, right, result);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		throw NotImplementedException("Unimplemented compare: COMPARE_DISTINCT_FROM");
	default:
		throw NotImplementedException("Unknown comparison type!");
	}
}
