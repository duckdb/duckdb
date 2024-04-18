#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

// if a filter expression is on one of the bindings in the partition set.
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

bool FilterPushdown::CanPushdownFilter(vector<column_binding_set_t> window_exprs_partition_bindings, vector<ColumnBinding> bindings) {
	bool ret = true;
	for (auto &partition_bindings : window_exprs_partition_bindings) {
		bool filter_on_partitions_bindings = true;
		for (auto &binding : bindings) {
			if (partition_bindings.find(binding) == partition_bindings.end()) {
				filter_on_partitions_bindings = false;
			}
		}
		ret = ret && filter_on_partitions_bindings;
	}
	return ret;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownWindow(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_WINDOW);
	auto &window = op->Cast<LogicalWindow>();
	// go into expressions, then look into the partitions.
	// if the filter applies to a partition in each window expression then you can push the filter
	// into the children.
	FilterPushdown pushdown(optimizer);
	vector<unique_ptr<Filter>> leftover_filters;

	// First loop through the window expressions. If all window expressions
	// have partitions, maybe we can push down a filter through the partition.
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

	// Loop through the filters. If a filter is on a partition in every window expression
	// it can be pushed down.
	for (idx_t i = 0; i < filters.size(); i++) {
		auto can_pushdown_filter = true;

		// the filter must be on all partition bindings
		vector<ColumnBinding> bindings;
		ExtractFilterBindings(*filters.at(i)->filter, bindings);
		if (CanPushdownFilter(window_exprs_partition_bindings, bindings)) {

		}
//		for (auto &partition_bindings : window_exprs_partition_bindings) {
//			can_pushdown_filter = can_pushdown_filter && FilterIsOnPartition(partition_bindings, *filters.at(i)->filter);
//		}
//		if (can_pushdown_filter) {
//			pushdown.filters.push_back(std::move(filters.at(i)));
//		} else {
//			leftover_filters.push_back(std::move(filters.at(i)));
//		}
	}
	op->children[0] = pushdown.Rewrite(std::move(op->children[0]));
	filters = std::move(leftover_filters);
	return FinishPushdown(std::move(op));

}
} // namespace duckdb
