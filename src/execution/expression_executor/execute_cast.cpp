#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundCastExpression &expr, Vector &result) {
	// resolve the child
	Vector child;
	Execute(*expr.child, child);
	if (child.type == expr.return_type) {
		// NOP cast
		child.Move(result);
	} else {
		// cast it to the type specified by the cast expression
		result.Initialize(expr.return_type);
		VectorOperations::Cast(child, result, expr.source_type, expr.target_type);
	}
}
