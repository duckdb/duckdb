#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindPositionalReference(const PositionalReferenceExpression &ref, idx_t depth,
                                                     bool root_expression) {
	// Resolve the position against this binder, which owns the positions. The reference can reach this point from an
	// inner scope through alias expansion, e.g. "SELECT #2 AS x FROM t WHERE (SELECT x)" copies "#2" into the
	// subquery; it then binds here at depth > 0 and must be resolved as a correlated reference to the outer position.
	auto column = binder.bind_context.PositionToColumn(ref);
	return BindExpression(*column, depth, root_expression);
}

} // namespace duckdb
