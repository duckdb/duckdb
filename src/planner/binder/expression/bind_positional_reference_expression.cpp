#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(PositionalReferenceExpression &ref, idx_t depth) {
	if (depth != 0) {
		return BindResult("Positional reference expression could not be bound");
	}
	return binder.bind_context.BindColumn(ref, depth);
}

} // namespace duckdb
