#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

JoinFilterPushdownOptimizer::JoinFilterPushdownOptimizer(Optimizer &optimizer) : optimizer(optimizer) {
}

void JoinFilterPushdownOptimizer::GenerateJoinFilters(LogicalComparisonJoin &join) {
	switch (join.join_type) {
	case JoinType::MARK:
	case JoinType::SINGLE:
	case JoinType::LEFT:
	case JoinType::OUTER:
	case JoinType::ANTI:
	case JoinType::RIGHT_ANTI:
	case JoinType::RIGHT_SEMI:
		// cannot generate join filters for these join types
		// mark/single - cannot change cardinality of probe side
		// left/outer always need to include every row from probe side
		// FIXME: anti/right_anti - we could do this, but need to invert the join conditions
		return;
	default:
		break;
	}
	// re-order conditions here - otherwise this will happen later on and invalidate the indexes we generate
	PhysicalComparisonJoin::ReorderConditions(join.conditions);
	auto pushdown_info = make_uniq<JoinFilterPushdownInfo>();
	for (idx_t cond_idx = 0; cond_idx < join.conditions.size(); cond_idx++) {
		auto &cond = join.conditions[cond_idx];
		if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
			// only equality supported for now
			continue;
		}
		if (cond.left->type != ExpressionType::BOUND_COLUMN_REF) {
			// only bound column ref supported for now
			continue;
		}
		if (cond.left->return_type.IsNested()) {
			// nested columns are not supported for pushdown
			continue;
		}
		if (cond.left->return_type.id() == LogicalTypeId::INTERVAL) {
			// interval is not supported for pushdown
			continue;
		}
		JoinFilterPushdownColumn pushdown_col;
		pushdown_col.join_condition = cond_idx;

		auto &colref = cond.left->Cast<BoundColumnRefExpression>();
		pushdown_col.probe_column_index = colref.binding;
		pushdown_info->filters.push_back(pushdown_col);
	}
	if (pushdown_info->filters.empty()) {
		// could not generate any filters - bail-out
		return;
	}
	// find the child LogicalGet (if possible)
	reference<LogicalOperator> probe_source(*join.children[0]);
	while (probe_source.get().type != LogicalOperatorType::LOGICAL_GET) {
		auto &probe_child = probe_source.get();
		switch (probe_child.type) {
		case LogicalOperatorType::LOGICAL_LIMIT:
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_ORDER_BY:
		case LogicalOperatorType::LOGICAL_TOP_N:
		case LogicalOperatorType::LOGICAL_DISTINCT:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
			// does not affect probe side - continue into left child
			// FIXME: we can probably recurse into more operators here (e.g. window, set operation, unnest)
			probe_source = *probe_child.children[0];
			break;
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			// projection - check if we all of the expressions are only column references
			auto &proj = probe_source.get().Cast<LogicalProjection>();
			for (auto &filter : pushdown_info->filters) {
				if (filter.probe_column_index.table_index != proj.table_index) {
					// index does not belong to this projection - bail-out
					return;
				}
				auto &expr = *proj.expressions[filter.probe_column_index.column_index];
				if (expr.type != ExpressionType::BOUND_COLUMN_REF) {
					// not a simple column ref - bail-out
					return;
				}
				// column-ref - pass through the new column binding
				auto &colref = expr.Cast<BoundColumnRefExpression>();
				filter.probe_column_index = colref.binding;
			}
			probe_source = *probe_child.children[0];
			break;
		}
		default:
			// unsupported child type
			return;
		}
	}
	// found the LogicalGet
	auto &get = probe_source.get().Cast<LogicalGet>();
	if (!get.function.filter_pushdown) {
		// filter pushdown is not supported - bail-out
		return;
	}
	for (auto &filter : pushdown_info->filters) {
		if (filter.probe_column_index.table_index != get.table_index) {
			// the filter does not apply to the probe side here - bail-out
			return;
		}
	}
	// pushdown can be performed

	// set up the min/max aggregates for each of the filters
	vector<AggregateFunction> aggr_functions;
	aggr_functions.push_back(MinFun::GetFunction());
	aggr_functions.push_back(MaxFun::GetFunction());
	for (auto &filter : pushdown_info->filters) {
		for (auto &aggr : aggr_functions) {
			FunctionBinder function_binder(optimizer.GetContext());
			vector<unique_ptr<Expression>> aggr_children;
			aggr_children.push_back(join.conditions[filter.join_condition].right->Copy());
			auto aggr_expr = function_binder.BindAggregateFunction(aggr, std::move(aggr_children), nullptr,
			                                                       AggregateType::NON_DISTINCT);
			if (aggr_expr->children.size() != 1) {
				// min/max with collation - not supported
				return;
			}
			pushdown_info->min_max_aggregates.push_back(std::move(aggr_expr));
		}
	}
	// set up the dynamic filters (if we don't have any yet)
	if (!get.dynamic_filters) {
		get.dynamic_filters = make_shared_ptr<DynamicTableFilterSet>();
	}
	pushdown_info->dynamic_filters = get.dynamic_filters;

	// set up the filter pushdown in the join itself
	join.filter_pushdown = std::move(pushdown_info);
}

void JoinFilterPushdownOptimizer::VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		// comparison join - try to generate join filters (if possible)
		GenerateJoinFilters(op.Cast<LogicalComparisonJoin>());
	}
	LogicalOperatorVisitor::VisitOperator(op);
}

} // namespace duckdb
