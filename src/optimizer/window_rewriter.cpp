#include "duckdb/optimizer/window_rewriter.hpp"

namespace duckdb {

WindowRewriter::WindowRewriter(Optimizer &optimizer) : optimizer(optimizer), lhs_window(false) {
}

bool WindowRewriter::CanOptimize(LogicalOperator &op) {
	// FIXME Not the most elegant way
	if (op.type == LogicalOperatorType::LOGICAL_JOIN || op.type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	    op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN ||
	    op.type == LogicalOperatorType::LOGICAL_JOIN && !op.children.empty()) {
		auto &join_op = op.Cast<LogicalJoin>();
		auto *child = join_op.children[0].get();
		while (child) {
			if (child->type == LogicalOperatorType::LOGICAL_WINDOW) {
				lhs_window = true;
				break;
			}
			child = child->children[0].get();
		}
	}

	// If the operator is a projection and its child is a window, check if optimization is possible
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION &&
	    op.children[0]->type == LogicalOperatorType::LOGICAL_WINDOW && !lhs_window) {

		auto *child = op.children[0].get();
		auto &window = child->Cast<LogicalWindow>();

		// Only support ROW_NUMBER() OVER () with no PARTITION or ORDER BY
		if (window.expressions.size() != 1) {
			return false;
		}

		auto &expression = window.expressions[0];
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

	// Extend child LogicalGet output with virtual row_number column
	auto column_ids = get.GetColumnIds();
	// FIXME I think it is safe to add the row_number virtual column last, but check!
	idx_t row_number_index = column_ids.size();
	column_ids.emplace_back(ColumnIndex(COLUMN_IDENTIFIER_ROW_NUMBER));
	get.SetColumnIds(std::move(column_ids));
	get.types.push_back(LogicalType::BIGINT);
	get.projection_ids.push_back(row_number_index);

	const auto child_bindings = get.GetColumnBindings();
	const auto child_types = get.types;
	auto proj_index = child_bindings[0].table_index;

	// Replace WINDOW + PROJECTION with new projection
	const auto old_window_bindings = proj.GetColumnBindings();

	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(row_number_index + 1);

	// Copy all original child columns
	for (idx_t i = 0; i < row_number_index; ++i) {
		expressions.push_back(make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]));
	}

	// Add virtual row_number reference
	auto row_number_binding = ColumnBinding(proj_index, row_number_index);
	expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, row_number_binding));

	// New projection gets its own table index
	auto new_proj_index = optimizer.binder.GenerateTableIndex();
	auto new_projection = make_uniq<LogicalProjection>(new_proj_index, std::move(expressions));
	new_projection->children.push_back(std::move(child));

	// Update column binding replacer mapping
	const auto new_projection_bindings = new_projection->GetColumnBindings();
	D_ASSERT(new_projection_bindings.size() == old_window_bindings.size());
	for (idx_t i = 0; i < old_window_bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(old_window_bindings[i], new_projection_bindings[i]);
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
