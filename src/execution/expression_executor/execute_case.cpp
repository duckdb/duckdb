#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/case_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionExecutor::Visit(CaseExpression &expr) {
	if (expr.children.size() != 3) {
		throw Exception("Cast needs three child nodes");
	}
	Vector check, res_true, res_false;
	expr.children[0]->Accept(this);
	vector.Move(check);
	expr.children[1]->Accept(this);
	vector.Move(res_true);
	expr.children[2]->Accept(this);
	vector.Move(res_false);

	vector.Initialize(res_true.type);
	VectorOperations::Case(check, res_true, res_false, vector);
	expr.stats.Verify(vector);
	return nullptr;
}
