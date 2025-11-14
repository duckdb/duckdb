#include "duckdb/optimizer/window_rewriter.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

bool WindowRewriter::CanOptimize(LogicalOperator &op) {
	// If the operator is a window function and its child is a get, check if optimization is possible
	if (op.type == LogicalOperatorType::LOGICAL_WINDOW) {
		if (op.expressions.size() != 1) {
			return false;
		}

		auto &expression = op.expressions[0];
		auto &window_expr = expression->Cast<BoundWindowExpression>();

		// Try to optimize simple window functions, without partitions or ordering
		if (!window_expr.partitions.empty() || !window_expr.orders.empty()) {
			return false;
		}
		if (expression->type != ExpressionType::WINDOW_ROW_NUMBER) {
			return false;
		}

		// Should be followed by a get
		auto &window_ch = op.children[0];
		if (window_ch->type != LogicalOperatorType::LOGICAL_GET) {
			return false;
		}

		// and can only be a seq_scan
		auto &get = window_ch->Cast<LogicalGet>();
		if (get.function.name != "seq_scan") {
			return false;
		}

		return true;
	}

	return false;
}

unique_ptr<LogicalOperator> WindowRewriter::Optimize(unique_ptr<LogicalOperator> op) {
	ColumnBindingReplacer replacer;
	LogicalOperator *root = op.get();
	op = RewritePlan(std::move(op), replacer);

	if (!replacer.replacement_bindings.empty()) {
		replacer.VisitOperator(*root);
	}

	return op;
}

unique_ptr<LogicalOperator> WindowRewriter::RewriteGet(unique_ptr<LogicalOperator> op,
                                                       ColumnBindingReplacer &replacer) {
	auto &window = op->Cast<LogicalWindow>();
	auto &child = window.children[0];
	auto &get = child->Cast<LogicalGet>();

	// Extend child LogicalGet output with virtual row_number column
	auto column_ids = get.GetColumnIds();
	auto types = get.types;
	auto projection_ids = get.projection_ids;

	if (projection_ids.size() == 0) {
		column_ids.clear();
		types.clear();
	}
	column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_NUMBER);
	types.push_back(LogicalType::BIGINT);
	projection_ids.push_back(column_ids.size() - 1);

	get.SetColumnIds(std::move(column_ids));
	get.types = std::move(types);
	get.projection_ids = std::move(projection_ids);

	const auto child_bindings = get.GetColumnBindings();

	// Remove WINDOW and update bindings
	const auto old_window_bindings = window.GetColumnBindings();

	for (idx_t i = 0; i < old_window_bindings.size(); i++) {
		ColumnBinding target_binding;
		if (i < child_bindings.size() - 1) {
			// Map existing columns
			target_binding = child_bindings[i];
		} else {
			// Map the virtual ROW_NUMBER column
			target_binding = child_bindings.back();
		}
		replacer.replacement_bindings.emplace_back(old_window_bindings[i], target_binding);
	}

	replacer.stop_operator = child.get();
	return std::move(window.children[0]);
}

unique_ptr<LogicalOperator> WindowRewriter::RewritePlan(unique_ptr<LogicalOperator> op,
                                                        ColumnBindingReplacer &replacer) {
	if (CanOptimize(*op)) {
		return RewriteGet(std::move(op), replacer);
	}

	// Recurse into children
	for (auto &child : op->children) {
		child = RewritePlan(std::move(child), replacer);
	}
	return op;
}
} // namespace duckdb
