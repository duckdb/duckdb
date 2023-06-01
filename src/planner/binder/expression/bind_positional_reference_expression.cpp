#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindPositionalReference(unique_ptr<ParsedExpression> &expr, idx_t depth,
                                                     bool root_expression) {
	auto &ref = expr->Cast<PositionalReferenceExpression>();
	if (depth != 0) {
		throw InternalException("Positional reference expression could not be bound");
	}
	// replace the positional reference with a column
	auto column = binder.bind_context.PositionToColumn(ref);
	expr = std::move(column);
	return BindExpression(expr, depth, root_expression);
}

} // namespace duckdb
