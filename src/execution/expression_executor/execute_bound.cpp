#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/bound_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Visit(BoundExpression &expr) {
	assert(expr.index != (uint32_t)-1);
	assert(expr.index < chunk->column_count);
	vector.Reference(chunk->data[expr.index]);
	Verify(expr);
}
