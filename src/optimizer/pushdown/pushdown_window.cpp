#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

bool CanPushdownFilter(vector<column_binding_set_t> window_exprs_partition_bindings,
                       const vector<ColumnBinding> &bindings) {
	auto filter_on_all_partitions = true;
	for (auto &partition_binding_set : window_exprs_partition_bindings) {
		auto filter_on_binding_set = true;
		for (auto &binding : bindings) {
			if (partition_binding_set.find(binding) == partition_binding_set.end()) {
				filter_on_binding_set = false;
				break;
			}
		}
		filter_on_all_partitions = filter_on_all_partitions && filter_on_binding_set;
		if (!filter_on_all_partitions) {
			break;
		}
	}
	return filter_on_all_partitions;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownWindow(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_WINDOW);
	auto &window = op->Cast<LogicalWindow>();
	FilterPushdown pushdown(optimizer);

	// 1. Loop throguh the expressions, find the window expressions and investigate the partitions
	// if a filter applies to a partition in each window expression then you can push the filter
	// into the children.
	vector<column_binding_set_t> window_exprs_partition_bindings;
	for (auto &expr : window.expressions) {
		if (expr->expression_class != ExpressionClass::BOUND_WINDOW) {
			continue;
		}
		auto &window_expr = expr->Cast<BoundWindowExpression>();
		auto &partitions = window_expr.partitions;
		if (partitions.empty()) {
			// If any window expression does not have partitions, we cannot push any filters.
			// all window expressions need to be partitioned by the same column
			// in order to push down the window.
			return FinishPushdown(std::move(op));
		}
		column_binding_set_t partition_bindings;
		// 2. Get the binding information of the partitions of the window expression
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
		window_exprs_partition_bindings.push_back(partition_bindings);
	}

	if (window_exprs_partition_bindings.empty()) {
		return FinishPushdown(std::move(op));
	}

	vector<unique_ptr<Filter>> leftover_filters;
	// Loop through the filters. If a filter is on a partition in every window expression
	// it can be pushed down.
	for (idx_t i = 0; i < filters.size(); i++) {
		// the filter must be on all partition bindings
		vector<ColumnBinding> bindings;
		ExtractFilterBindings(*filters.at(i)->filter, bindings);
		if (CanPushdownFilter(window_exprs_partition_bindings, bindings)) {
			pushdown.filters.push_back(std::move(filters.at(i)));
		} else {
			leftover_filters.push_back(std::move(filters.at(i)));
		}
	}
	op->children[0] = pushdown.Rewrite(std::move(op->children[0]));
	filters = std::move(leftover_filters);
	return FinishPushdown(std::move(op));
}
} // namespace duckdb
