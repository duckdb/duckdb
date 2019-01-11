#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/case_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Visit(CaseExpression &expr) {
	Vector check, res_true, res_false;
	expr.check->Accept(this);
	vector.Move(check);
	expr.result_if_true->Accept(this);
	vector.Move(res_true);
	expr.result_if_false->Accept(this);
	vector.Move(res_false);

	vector.Initialize(res_true.type);
	VectorOperations::Case(check, res_true, res_false, vector);
	Verify(expr);
}
