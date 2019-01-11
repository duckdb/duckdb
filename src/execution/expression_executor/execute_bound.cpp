#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/bound_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Visit(BoundExpression &expr) {
	size_t cur_depth = expr.depth;
	ExpressionExecutor *cur_exec = this;
	while (cur_depth > 0) {
		cur_exec = cur_exec->parent;
		assert(cur_exec);
		cur_depth--;
	}
	assert(expr.index != (size_t) -1);
	assert(expr.index < cur_exec->chunk->column_count);

	vector.Reference(cur_exec->chunk->data[expr.index]);
	Verify(expr);
}
