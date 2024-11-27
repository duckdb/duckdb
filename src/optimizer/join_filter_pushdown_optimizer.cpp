#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"

#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"

namespace duckdb {

JoinFilterPushdownOptimizer::JoinFilterPushdownOptimizer(Optimizer &optimizer) : optimizer(optimizer) {
}

bool PushdownJoinFilterExpression(Expression &expr, JoinFilterPushdownColumn &filter) {
	if (expr.type != ExpressionType::BOUND_COLUMN_REF) {
		// not a simple column ref - bail-out
		return false;
	}
	// column-ref - pass through the new column binding
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	filter.probe_column_index = colref.binding;
	return true;
}

void GenerateJoinFiltersRecursive(LogicalOperator &op, vector<JoinFilterPushdownColumn> columns,
                                  JoinFilterPushdownInfo &pushdown_info) {
	auto &probe_child = op;
	switch (probe_child.type) {
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		// does not affect probe side - recurse into left child
		// FIXME: we can probably recurse into more operators here (e.g. window, unnest)
		GenerateJoinFiltersRecursive(*probe_child.children[0], std::move(columns), pushdown_info);
		break;
	case LogicalOperatorType::LOGICAL_UNNEST: {
		auto &unnest = probe_child.Cast<LogicalUnnest>();
		// check if the filters apply to the unnest index
		for (auto &filter : columns) {
			if (filter.probe_column_index.table_index == unnest.unnest_index) {
				// the filter applies to the unnest index - bail out
				return;
			}
		}
		GenerateJoinFiltersRecursive(*probe_child.children[0], std::move(columns), pushdown_info);
		break;
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNION: {
		auto &setop = probe_child.Cast<LogicalSetOperation>();
		// union
		// check if the filters apply to this table index
		for (auto &filter : columns) {
			if (filter.probe_column_index.table_index != setop.table_index) {
				// the filter does not apply to the union - bail-out
				return;
			}
		}
		for (auto &child : probe_child.children) {
			// rewrite the filters for each of the children of the union
			vector<JoinFilterPushdownColumn> child_columns;
			auto child_bindings = child->GetColumnBindings();
			child_columns.reserve(columns.size());
			for (auto &child_column : columns) {
				JoinFilterPushdownColumn new_col;
				new_col.probe_column_index = child_bindings[child_column.probe_column_index.column_index];
				child_columns.push_back(new_col);
			}
			// then recurse into the child
			GenerateJoinFiltersRecursive(*child, std::move(child_columns), pushdown_info);

			// for EXCEPT we can only recurse into the first (left) child
			if (probe_child.type == LogicalOperatorType::LOGICAL_EXCEPT) {
				break;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// found LogicalGet
		auto &get = probe_child.Cast<LogicalGet>();
		if (!get.function.filter_pushdown) {
			// filter pushdown is not supported - no need to consider this node
			return;
		}
		for (auto &filter : columns) {
			if (filter.probe_column_index.table_index != get.table_index) {
				// the filter does not apply to the probe side here - bail-out
				return;
			}
		}
		// pushdown info can be applied to this LogicalGet - push the dynamic table filter set
		if (!get.dynamic_filters) {
			get.dynamic_filters = make_shared_ptr<DynamicTableFilterSet>();
		}

		JoinFilterPushdownFilter get_filter;
		get_filter.dynamic_filters = get.dynamic_filters;
		get_filter.columns = std::move(columns);
		pushdown_info.probe_info.push_back(std::move(get_filter));
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// projection - check if we all of the expressions are only column references
		auto &proj = probe_child.Cast<LogicalProjection>();
		for (auto &filter : columns) {
			if (filter.probe_column_index.table_index != proj.table_index) {
				// index does not belong to this projection - bail-out
				return;
			}
			auto &expr = *proj.expressions[filter.probe_column_index.column_index];
			if (!PushdownJoinFilterExpression(expr, filter)) {
				// cannot push through this expression - bail-out
				return;
			}
		}
		GenerateJoinFiltersRecursive(*probe_child.children[0], std::move(columns), pushdown_info);
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// we can push filters through aggregates IF they all point to groups
		auto &aggr = probe_child.Cast<LogicalAggregate>();
		for (auto &filter : columns) {
			if (filter.probe_column_index.table_index != aggr.group_index) {
				// index does not refer to a group - bail-out
				return;
			}
			auto &expr = *aggr.groups[filter.probe_column_index.column_index];
			if (!PushdownJoinFilterExpression(expr, filter)) {
				// cannot push through this expression - bail-out
				return;
			}
		}
		GenerateJoinFiltersRecursive(*probe_child.children[0], std::move(columns), pushdown_info);
		break;
	}
	default:
		// unsupported child type
		break;
	}
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

	vector<JoinFilterPushdownColumn> pushdown_columns;
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
		auto &colref = cond.left->Cast<BoundColumnRefExpression>();
		pushdown_col.probe_column_index = colref.binding;
		pushdown_columns.push_back(pushdown_col);

		pushdown_info->join_condition.push_back(cond_idx);
	}
	if (pushdown_columns.empty()) {
		// could not generate any filters - bail-out
		return;
	}
	// recurse the query tree to find the LogicalGets in which we can push the filter info
	GenerateJoinFiltersRecursive(*join.children[0], pushdown_columns, *pushdown_info);

	if (pushdown_info->probe_info.empty()) {
		// no table sources found in which we can push down filters
		return;
	}
	// set up the min/max aggregates for each of the filters
	vector<AggregateFunction> aggr_functions;
	aggr_functions.push_back(MinFunction::GetFunction());
	aggr_functions.push_back(MaxFunction::GetFunction());
	for (auto &join_condition : pushdown_info->join_condition) {
		for (auto &aggr : aggr_functions) {
			FunctionBinder function_binder(optimizer.GetContext());
			vector<unique_ptr<Expression>> aggr_children;
			aggr_children.push_back(join.conditions[join_condition].right->Copy());
			auto aggr_expr = function_binder.BindAggregateFunction(aggr, std::move(aggr_children), nullptr,
			                                                       AggregateType::NON_DISTINCT);
			if (aggr_expr->children.size() != 1) {
				// min/max with collation - not supported
				return;
			}
			pushdown_info->min_max_aggregates.push_back(std::move(aggr_expr));
		}
	}
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
