#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

static bool FilterIsOnPartition(column_binding_set_t partition_bindings, Expression &filter_expression) {
	bool filter_is_on_partition = false;
	ExpressionIterator::EnumerateChildren(filter_expression, [&](unique_ptr<Expression> &child) {
		switch (child->type) {
		case ExpressionType::BOUND_COLUMN_REF: {
			auto &col_ref = child->Cast<BoundColumnRefExpression>();
			if (partition_bindings.find(col_ref.binding) != partition_bindings.end()) {
				filter_is_on_partition = true;
				// if the filter has more columns, break to make sure the other
				// columns of the filter are also partitioned on.
				break;
			}
			// the filter is not on a column that is partitioned. immediately return.
			filter_is_on_partition = false;
			return;
		}
		// for simplicity, we don't push down filters that are functions.
		// CONJUNCTION_AND filters are also not pushed down
		case ExpressionType::BOUND_FUNCTION:
		case ExpressionType::CONJUNCTION_AND: {
			filter_is_on_partition = false;
			return;
		}
		default:
			break;
		}
	});
	return filter_is_on_partition;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownWindow(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_WINDOW);
	auto &window = op->Cast<LogicalWindow>();
	// go into expressions, then look into the partitions.
	// if the filter applies to a partition in each window expression then you can push the filter
	// into the children.
	auto pushdown = FilterPushdown(optimizer);
	vector<int8_t> filters_to_pushdown;
	filters_to_pushdown.reserve(filters.size());
	for (idx_t i = 0; i < filters.size(); i++) {
		filters_to_pushdown.push_back(0);
	}
	// 1. Check every window expression
	for (auto &expr : window.expressions) {
		if (expr->expression_class != ExpressionClass::BOUND_WINDOW) {
			continue;
		}
		auto &window_expr = expr->Cast<BoundWindowExpression>();
		auto &partitions = window_expr.partitions;
		if (partitions.empty()) {
			// all window expressions need to be partitioned by the same column
			// in order to push down the window.
			return FinishPushdown(std::move(op));
		}
		column_binding_set_t partition_bindings;
		// 2. Get the binding information of the partitions
		for (auto &partition_expr : partitions) {
			switch (partition_expr->type) {
			// TODO: Add expressions for function expressions like FLOOR, CEIL etc.
			case ExpressionType::BOUND_COLUMN_REF: {
				auto &partition_col = partition_expr->Cast<BoundColumnRefExpression>();
				partition_bindings.insert(partition_col.binding);
				break;
			}
			default:
				break;
			}
		}

		// 3. Check if a filter is on one of the partitions
		//    in the event there are multiple window functions, a fiter can only be pushed down
		//    if each window function is partitioned on the filtered value
		//    if a filter is not on a partition filters_to_partition[filter_index] = -1
		//    which means the filter will no longer be pushed down, even if the filter is on a partition.
		//    tpcds q47 caught this
		for (idx_t i = 0; i < filters.size(); i++) {
			auto &filter = filters.at(i);
			if (FilterIsOnPartition(partition_bindings, *filter->filter) && filters_to_pushdown[i] >= 0) {
				filters_to_pushdown[i] = 1;
				continue;
			}
			filters_to_pushdown[i] = -1;
		}
	}
	// push the new filters into the child.
	vector<unique_ptr<Filter>> leftover_filters;
	for (idx_t i = 0; i < filters.size(); i++) {
		if (filters_to_pushdown[i] == 1) {
			pushdown.filters.push_back(std::move(filters.at(i)));
			continue;
		}
		leftover_filters.push_back(std::move(filters.at(i)));
	}
	if (!pushdown.filters.empty()) {
		op->children[0] = pushdown.Rewrite(std::move(op->children[0]));
	}
	filters = std::move(leftover_filters);

	return FinishPushdown(std::move(op));
}
} // namespace duckdb
