#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundConstantExpression &expr, Vector &result) {
	result.Reference(expr.value);
}
