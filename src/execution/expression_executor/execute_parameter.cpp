#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_parameter_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundParameterExpression &expr, Vector &result) {
	if (expr.return_type == TypeId::INVALID) {
		throw Exception("Stil missing a type for parameter");
	}
	result.Reference(expr.value);
}
