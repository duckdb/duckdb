#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_case_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundCaseExpression &expr, Vector &result) {
	Vector check, res_true, res_false;

	Execute(*expr.check, check);
	Execute(*expr.result_if_true, res_true);
	Execute(*expr.result_if_false, res_false);

	result.Initialize(res_true.type);
	VectorOperations::Case(check, res_true, res_false, result);
}
