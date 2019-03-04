#include "optimizer/filter_pushdown.hpp"

#include "planner/operator/logical_projection.hpp"

using namespace duckdb;
using namespace std;

static unique_ptr<Expression> ReplaceProjectionBindings(LogicalProjection &proj, unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)*expr;
		assert(colref.binding.table_index == proj.table_index);
		assert(colref.binding.column_index < proj.expressions.size());
		assert(colref.depth == 0);
		// replace the binding with a copy to the expression at the referenced index
		return proj.expressions[colref.binding.column_index]->Copy();
	}
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return ReplaceProjectionBindings(proj, move(child));
	});
	return expr;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownProjection(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::PROJECTION);
	auto &proj = (LogicalProjection&) *op;
	// push filter through logical projection
	// all the BoundColumnRefExpressions in the filter should refer to the LogicalProjection
	// we can rewrite them by replacing those references with the expression of the LogicalProjection node
	for(size_t i = 0; i < filters.size(); i++) {
		auto &f = *filters[i];
		assert(f.bindings.size() <= 1);
		f.bindings.clear();
		// rewrite the bindings within this subquery
		f.filter = ReplaceProjectionBindings(proj, move(f.filter));
		// extract the bindings again
		f.ExtractBindings();
	}
	// now push into children
	proj.children[0] = Rewrite(move(proj.children[0]));
	return op;
}
