#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/parameter_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Visit(ParameterExpression &expr) {
	if (expr.return_type == TypeId::INVALID) {
		throw Exception("Stil missing a type for parameter");
	}
	vector.Reference(expr.value);
	Verify(expr);
}
