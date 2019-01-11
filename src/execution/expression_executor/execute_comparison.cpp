#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/comparison_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Visit(ComparisonExpression &expr) {
	Vector l, r;
	expr.left->Accept(this);
	vector.Move(l);
	expr.right->Accept(this);
	vector.Move(r);
	vector.Initialize(TypeId::BOOLEAN);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(l, r, vector);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(l, r, vector);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(l, r, vector);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(l, r, vector);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(l, r, vector);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(l, r, vector);
		break;
	case ExpressionType::COMPARE_LIKE:
		VectorOperations::Like(l, r, vector);
		break;
	case ExpressionType::COMPARE_NOTLIKE:
		VectorOperations::NotLike(l, r, vector);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		throw NotImplementedException("Unimplemented compare: COMPARE_DISTINCT_FROM");
	default:
		throw NotImplementedException("Unknown comparison type!");
	}
	Verify(expr);
}
