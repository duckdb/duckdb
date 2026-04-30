#include "duckdb/optimizer/row_number_rewriter.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

bool RowNumberRewriter::CanOptimize(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_WINDOW) {
		return false;
	}
	if (op.expressions.size() != 1) {
		return false;
	}

	auto &expression = op.expressions[0];
	if (expression->GetExpressionType() != ExpressionType::WINDOW_ROW_NUMBER) {
		return false;
	}
	auto &window_expr = expression->Cast<BoundWindowExpression>();
	if (!window_expr.partitions.empty() || !window_expr.orders.empty()) {
		return false;
	}

	// Should be followed by a get
	auto &window_ch = op.children[0];
	if (window_ch->type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}

	auto &get = window_ch->Cast<LogicalGet>();

	// the get must support the row_number virtual column
	if (get.virtual_columns.find(COLUMN_IDENTIFIER_ROW_NUMBER) == get.virtual_columns.end()) {
		return false;
	}

	// cannot rewrite if there are table filters pushed into the scan
	// the virtual row_number column counts absolute row positions in storage,
	// which does not match row_number() over() when rows are filtered out
	if (get.table_filters.HasFilters()) {
		return false;
	}

	return true;
}

unique_ptr<LogicalOperator> RowNumberRewriter::Optimize(unique_ptr<LogicalOperator> op) {
	ColumnBindingReplacer replacer;
	LogicalOperator *root = op.get();
	op = RewritePlan(std::move(op), replacer);

	if (!replacer.replacement_bindings.empty()) {
		replacer.VisitOperator(*root);
	}

	return op;
}

unique_ptr<LogicalOperator> RowNumberRewriter::RewriteGet(unique_ptr<LogicalOperator> op,
                                                          ColumnBindingReplacer &replacer) {
	auto &window = op->Cast<LogicalWindow>();
	auto &child = window.children[0];
	auto &get = child->Cast<LogicalGet>();

	// Add virtual row_number column to the LogicalGet
	auto proj_index = get.AddColumnId(COLUMN_IDENTIFIER_ROW_NUMBER);
	if (!get.projection_ids.empty()) {
		get.projection_ids.push_back(proj_index);
	}
	get.types.push_back(LogicalType::BIGINT);

	const auto child_bindings = get.GetColumnBindings();

	// Get the old window bindings before we remove the window operator
	const auto old_window_bindings = window.GetColumnBindings();

	// Map old window bindings to new get bindings
	for (idx_t i = 0; i < old_window_bindings.size(); i++) {
		ColumnBinding target_binding;
		if (i < child_bindings.size() - 1) {
			// Map existing columns through
			target_binding = child_bindings[i];
		} else {
			// Map the window expression to the virtual ROW_NUMBER column
			target_binding = child_bindings.back();
		}
		replacer.replacement_bindings.emplace_back(old_window_bindings[i], target_binding);
	}

	replacer.stop_operator = child.get();
	return std::move(window.children[0]);
}

unique_ptr<LogicalOperator> RowNumberRewriter::RewritePlan(unique_ptr<LogicalOperator> op,
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
