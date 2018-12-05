#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/cast_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionExecutor::Visit(CastExpression &expr) {
	// resolve the child
	Vector l;
	expr.children[0]->Accept(this);
	if (vector.type == expr.return_type) {
		// NOP cast
		return nullptr;
	}
	vector.Move(l);
	// now cast it to the type specified by the cast expression
	vector.Initialize(expr.return_type);
	VectorOperations::Cast(l, vector);
	expr.stats.Verify(vector);
	return nullptr;
}
