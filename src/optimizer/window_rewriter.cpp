#include "duckdb/optimizer/window_rewriter.hpp"

namespace duckdb {

WindowRewriter::WindowRewriter(Optimizer &optimizer) : optimizer(optimizer), lhs_window(false) {
}

bool WindowRewriter::CanOptimize(LogicalOperator &op) {

	// Check for window on LHS of join
	if (op.type == LogicalOperatorType::LOGICAL_JOIN || op.type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	    op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {

		auto &join_op = op.Cast<LogicalJoin>();
		if (join_op.children.empty()) {
			return false;
		}

		auto *child = join_op.children[0].get();
		while (child) {
			if (child->type == LogicalOperatorType::LOGICAL_WINDOW) {
				lhs_window = true;
				break;
			}
			if (child->children.empty())
				break;
			child = child->children[0].get();
		}
	}

	// If the operator is a projection and its child is a window, check if optimization is possible
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION && !lhs_window) {
		if (op.children.empty() || !op.children[0]) {
			return false;
		}
		if (op.children[0]->type != LogicalOperatorType::LOGICAL_WINDOW) {
			return false;
		}

		auto *child = op.children[0].get();
		auto &window = child->Cast<LogicalWindow>();

		if (window.expressions.size() != 1) {
			return false;
		}

		auto &expression = window.expressions[0];
		auto &window_expr = expression->Cast<BoundWindowExpression>();
		if (!window_expr.partitions.empty() || !window_expr.orders.empty()) {
			return false;
		}
		if (expression->type != ExpressionType::WINDOW_ROW_NUMBER) {
			return false;
		}

		return true;
	}

	return false;
}

unique_ptr<LogicalOperator> WindowRewriter::Optimize(unique_ptr<LogicalOperator> op) {

	ColumnBindingReplacer replacer;
	op = OptimizeInternal(std::move(op), replacer);

	if (!replacer.replacement_bindings.empty()) {
		replacer.VisitOperator(*op);
	}

	return op;
}

unique_ptr<LogicalOperator> WindowRewriter::RewriteGet(unique_ptr<LogicalOperator> op,
                                                       ColumnBindingReplacer &replacer) {
	auto &proj = op->Cast<LogicalProjection>();
	auto &window = proj.children[0]->Cast<LogicalWindow>();
	auto &child = window.children[0];

	auto &get = child->Cast<LogicalGet>();
	if (get.function.name != "seq_scan") {
		return op;
	}

	// Find where in the projection the row_number column appears
	idx_t row_number_index = 0;
	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		auto &col = proj.expressions.at(i);
		auto &col_ref = col->Cast<BoundColumnRefExpression>();
		if (col_ref.binding.table_index == window.window_index) {
			row_number_index = i;
			break;
		}
	}

	// Extend child LogicalGet output with virtual row_number column
	auto column_ids = get.GetColumnIds();
	auto types = get.types;
	auto projection_ids = get.projection_ids;

	column_ids.emplace_back(ColumnIndex(COLUMN_IDENTIFIER_ROW_NUMBER));
	types.push_back(LogicalType::BIGINT);
	projection_ids.push_back(column_ids.size() - 1);

	get.SetColumnIds(std::move(column_ids));
	get.types = std::move(types);
	get.projection_ids = std::move(projection_ids);

	const auto child_bindings = get.GetColumnBindings();
	const auto child_types = get.types;

	// Replace WINDOW + PROJECTION with new PROJECTION
	const auto old_projection_bindings = proj.GetColumnBindings();

	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(child_bindings.size());

	// Build projection expressions in correct order
	for (idx_t i = 0; i < proj.expressions.size(); ++i) {
		if (i == row_number_index) {
			// Add virtual ROW_NUMBER() reference at this position
			auto &row_number_binding = child_bindings.back();
			expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, row_number_binding));
		} else {
			// Copy the existing projection
			auto &col_ref = proj.expressions[i]->Cast<BoundColumnRefExpression>();
			auto binding_index = col_ref.binding.column_index;
			expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(child_types[binding_index], child_bindings[binding_index]));
		}
	}

	// Create the new projection
	auto new_proj_index = optimizer.binder.GenerateTableIndex();
	auto new_projection = make_uniq<LogicalProjection>(new_proj_index, std::move(expressions));
	new_projection->children.push_back(std::move(child));

	// Update column binding replacer mapping
	const auto new_projection_bindings = new_projection->GetColumnBindings();
	D_ASSERT(new_projection_bindings.size() == old_projection_bindings.size());
	for (idx_t i = 0; i < old_projection_bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(old_projection_bindings[i], new_projection_bindings[i]);
	}

	replacer.stop_operator = new_projection.get();
	return std::move(new_projection);
}

unique_ptr<LogicalOperator> WindowRewriter::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                             ColumnBindingReplacer &replacer) {

	if (CanOptimize(*op)) {
		auto &proj = op->Cast<LogicalProjection>();
		auto &window = proj.children[0]->Cast<LogicalWindow>();

		auto &child = window.children[0];

		switch (child->type) {
		case LogicalOperatorType::LOGICAL_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		case LogicalOperatorType::LOGICAL_DELIM_JOIN:
			// FIXME implement RewriteJoins
			break;
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
			// FIXME implement RewriteAggregate
			break;
		case LogicalOperatorType::LOGICAL_GET:
			return RewriteGet(std::move(op), replacer);
		default:
			break;
		}
	}

	// Recurse into children
	for (auto &child : op->children) {
		child = OptimizeInternal(std::move(child), replacer);
	}
	return op;
}
} // namespace duckdb
