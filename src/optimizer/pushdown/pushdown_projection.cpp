#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

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
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return ReplaceProjectionBindings(proj, move(child));
	});
	return expr;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownProjection(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::PROJECTION);
	auto &proj = (LogicalProjection &)*op;
	// push filter through logical projection
	// all the BoundColumnRefExpressions in the filter should refer to the LogicalProjection
	// we can rewrite them by replacing those references with the expression of the LogicalProjection node
	FilterPushdown child_pushdown(optimizer);
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &f = *filters[i];
		assert(f.bindings.size() <= 1);
		// rewrite the bindings within this subquery
		f.filter = ReplaceProjectionBindings(proj, move(f.filter));
		// add the filter to the child pushdown
		if (child_pushdown.AddFilter(move(f.filter)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_unique<LogicalEmptyResult>(move(op));
		}
	}
	child_pushdown.GenerateFilters();
	// now push into children
	op->children[0] = child_pushdown.Rewrite(move(op->children[0]));
	return op;
}
