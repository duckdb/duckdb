#include "optimizer/filter_pushdown.hpp"

#include "planner/operator/logical_aggregate.hpp"
#include "planner/operator/logical_join.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

static unique_ptr<Expression> ReplaceGroupBindings(LogicalAggregate &proj, unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)*expr;
		assert(colref.binding.table_index == proj.group_index);
		assert(colref.binding.column_index < proj.groups.size());
		assert(colref.depth == 0);
		// replace the binding with a copy to the expression at the referenced index
		return proj.groups[colref.binding.column_index]->Copy();
	}
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return ReplaceGroupBindings(proj, move(child));
	});
	return expr;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownAggregate(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY);
	auto &aggr = (LogicalAggregate&) *op;

	// pushdown into AGGREGATE and GROUP BY
	// we cannot push expressions that refer to the aggregate
	FilterPushdown child_pushdown(optimizer);
	for(size_t i = 0; i < filters.size(); i++) {
		auto &f = *filters[i];
		// check if the aggregate is in the set
		if (f.bindings.find(aggr.aggregate_index) == f.bindings.end()) {
			// no aggregate! we can push this down
			// rewrite any group bindings within the filter
			f.filter = ReplaceGroupBindings(aggr, move(f.filter));
			// extract the bindings again
			f.bindings.clear();
			LogicalJoin::GetExpressionBindings(*f.filter, f.bindings);
			// add the new filter to the child and erase the filter from here
			child_pushdown.filters.push_back(move(filters[i]));
			filters.erase(filters.begin() + i);
			i--;
		}
	}
	
	op->children[0] = child_pushdown.Rewrite(move(op->children[0]));
	return FinishPushdown(move(op));
}
