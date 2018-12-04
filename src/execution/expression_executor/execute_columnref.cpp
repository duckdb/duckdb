
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"

#include "parser/expression/columnref_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionExecutor::Visit(ColumnRefExpression &expr) {
	size_t cur_depth = expr.depth;
	ExpressionExecutor *cur_exec = this;
	while (cur_depth > 0) {
		cur_exec = cur_exec->parent;
		if (!cur_exec) {
			throw Exception("Unable to find matching parent executor");
		}
		cur_depth--;
	}

	if (expr.index == (size_t)-1) {
		throw Exception("Column Reference not bound!");
	}
	if (expr.index >= cur_exec->chunk->column_count) {
		throw Exception("Column reference index out of range!");
	}
	vector.Reference(cur_exec->chunk->data[expr.index]);
	expr.stats.Verify(vector);
	return nullptr;
}
