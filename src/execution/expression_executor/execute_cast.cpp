#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/cast_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Visit(CastExpression &expr) {
	// resolve the child
	Vector l;
	Execute(expr.child);
	if (vector.type == expr.return_type) {
		// NOP cast
		return;
	}
	vector.Move(l);
	// now cast it to the type specified by the cast expression
	vector.Initialize(expr.return_type);
	VectorOperations::Cast(l, vector);
	Verify(expr);
}
