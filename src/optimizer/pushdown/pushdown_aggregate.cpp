#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

static unique_ptr<Expression> ReplaceGroupBindings(LogicalAggregate &proj, unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)*expr;
		D_ASSERT(colref.binding.table_index == proj.group_index);
		D_ASSERT(colref.binding.column_index < proj.groups.size());
		D_ASSERT(colref.depth == 0);
		// replace the binding with a copy to the expression at the referenced index
		return proj.groups[colref.binding.column_index]->Copy();
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { child = ReplaceGroupBindings(proj, std::move(child)); });
	return expr;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownAggregate(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
	auto &aggr = (LogicalAggregate &)*op;

	// pushdown into AGGREGATE and GROUP BY
	// we cannot push expressions that refer to the aggregate
	FilterPushdown child_pushdown(optimizer);
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &f = *filters[i];
		if (f.bindings.find(aggr.aggregate_index) != f.bindings.end()) {
			// filter on aggregate: cannot pushdown
			continue;
		}
		if (f.bindings.find(aggr.groupings_index) != f.bindings.end()) {
			// filter on GROUPINGS function: cannot pushdown
			continue;
		}
		// if there are any empty grouping sets, we cannot push down filters
		bool has_empty_grouping_sets = false;
		for (auto &grp : aggr.grouping_sets) {
			if (grp.empty()) {
				has_empty_grouping_sets = true;
			}
		}
		if (has_empty_grouping_sets) {
			continue;
		}
		// no aggregate! we can push this down
		// rewrite any group bindings within the filter
		f.filter = ReplaceGroupBindings(aggr, std::move(f.filter));
		// add the filter to the child node
		if (child_pushdown.AddFilter(std::move(f.filter)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_unique<LogicalEmptyResult>(std::move(op));
		}
		// erase the filter from here
		filters.erase(filters.begin() + i);
		i--;
	}
	child_pushdown.GenerateFilters();

	op->children[0] = child_pushdown.Rewrite(std::move(op->children[0]));
	return FinishPushdown(std::move(op));
}

} // namespace duckdb
