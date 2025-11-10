#include "duckdb/optimizer/window_rewriter.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

WindowRewriter::WindowRewriter(Optimizer &optimizer) : optimizer(optimizer) {
}

bool WindowRewriter::CanOptimize(LogicalOperator &op) {
	// If the operator is a projection and its child is a window, check if optimization is possible
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
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

		// Try to optimize simple window functions, without partitions or ordering
		if (!window_expr.partitions.empty() || !window_expr.orders.empty()) {
			return false;
		}
		if (expression->type != ExpressionType::WINDOW_ROW_NUMBER) {
			return false;
		}

		// Should be followed by a get
		auto &window_ch = window.children[0];
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
	op = OptimizeInternal(std::move(op), replacer);

	if (!replacer.replacement_bindings.empty()) {
		replacer.VisitOperator(*op);
	}

	return op;
}

unique_ptr<LogicalOperator> WindowRewriter::Rewrite(unique_ptr<LogicalOperator> op, ColumnBindingReplacer &replacer) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_PROJECTION);
	auto &proj = op->Cast<LogicalProjection>();
	D_ASSERT(proj.children[0]->type == LogicalOperatorType::LOGICAL_WINDOW);
	auto &window = proj.children[0]->Cast<LogicalWindow>();
	auto &child = window.children[0];
	D_ASSERT(child->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = child->Cast<LogicalGet>();

	// Find where in the projection the row_number column appears
	idx_t row_number_index = 0;
	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		auto &col = proj.expressions.at(i);
		if (col->type != ExpressionType::BOUND_COLUMN_REF)
			continue;
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

	column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_NUMBER);
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
			// Copy the rest
			auto &expr = proj.expressions[i];
			expressions.push_back(expr->Copy());
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
		return Rewrite(std::move(op), replacer);
	}

	// Recurse into children
	for (auto &child : op->children) {
		child = OptimizeInternal(std::move(child), replacer);
	}
	return op;
}
} // namespace duckdb
