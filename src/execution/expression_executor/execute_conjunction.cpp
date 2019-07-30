#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundConjunctionExpression &expr, Vector &result) {
	Vector left, right;
	Execute(*expr.left, left);
	Execute(*expr.right, right);

	result.Initialize(TypeId::BOOLEAN);
	switch (expr.type) {
	case ExpressionType::CONJUNCTION_AND:
		VectorOperations::And(left, right, result);
		break;
	case ExpressionType::CONJUNCTION_OR:
		VectorOperations::Or(left, right, result);
		break;
	default:
		throw NotImplementedException("Unknown conjunction type!");
	}
}
