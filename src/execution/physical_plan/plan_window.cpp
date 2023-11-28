#include "duckdb/execution/operator/aggregate/physical_streaming_window.hpp"
#include "duckdb/execution/operator/aggregate/physical_window.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

#include <numeric>

namespace duckdb {

static bool IsStreamingWindow(unique_ptr<Expression> &expr) {
	auto &wexpr = expr->Cast<BoundWindowExpression>();
	if (!wexpr.partitions.empty() || !wexpr.orders.empty() || wexpr.ignore_nulls ||
	    wexpr.exclude_clause != WindowExcludeMode::NO_OTHER) {
		return false;
	}
	switch (wexpr.type) {
	// TODO: add more expression types here?
	case ExpressionType::WINDOW_AGGREGATE:
		// We can stream aggregates if they are "running totals" and don't use filters
		return wexpr.start == WindowBoundary::UNBOUNDED_PRECEDING && wexpr.end == WindowBoundary::CURRENT_ROW_ROWS &&
		       !wexpr.filter_expr;
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_ROW_NUMBER:
		return true;
	default:
		return false;
	}
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalWindow &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
#ifdef DEBUG
	for (auto &expr : op.expressions) {
		D_ASSERT(expr->IsWindow());
	}
#endif

	op.estimated_cardinality = op.EstimateCardinality(context);

	// Slice types
	auto types = op.types;
	const auto input_width = types.size() - op.expressions.size();
	types.resize(input_width);

	// Identify streaming windows
	vector<idx_t> blocking_windows;
	vector<idx_t> streaming_windows;
	for (idx_t expr_idx = 0; expr_idx < op.expressions.size(); expr_idx++) {
		if (IsStreamingWindow(op.expressions[expr_idx])) {
			streaming_windows.push_back(expr_idx);
		} else {
			blocking_windows.push_back(expr_idx);
		}
	}

	// Process the window functions by sharing the partition/order definitions
	unordered_map<idx_t, idx_t> projection_map;
	auto output_pos = input_width;
	while (!blocking_windows.empty() || !streaming_windows.empty()) {
		const bool process_streaming = blocking_windows.empty();
		auto &remaining = process_streaming ? streaming_windows : blocking_windows;

		// Find all functions that share the partitioning of the first remaining expression
		const auto over_idx = remaining[0];
		auto &over_expr = op.expressions[over_idx]->Cast<BoundWindowExpression>();

		vector<idx_t> matching;
		vector<idx_t> unprocessed;
		for (const auto &expr_idx : remaining) {
			D_ASSERT(op.expressions[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto &wexpr = op.expressions[expr_idx]->Cast<BoundWindowExpression>();
			if (expr_idx != over_idx && over_expr.Equals(wexpr)) {
				// CSE Elimination
				projection_map[input_width + expr_idx] = output_pos;
			} else if (over_expr.KeysAreCompatible(wexpr)) {
				matching.emplace_back(expr_idx);
			} else {
				unprocessed.emplace_back(expr_idx);
			}
		}
		remaining.swap(unprocessed);

		// Extract the matching expressions
		vector<unique_ptr<Expression>> select_list;
		for (const auto &expr_idx : matching) {
			select_list.emplace_back(std::move(op.expressions[expr_idx]));
			types.emplace_back(op.types[input_width + expr_idx]);
		}

		// Chain the new window operator on top of the plan
		unique_ptr<PhysicalOperator> window;
		if (process_streaming) {
			window = make_uniq<PhysicalStreamingWindow>(types, std::move(select_list), op.estimated_cardinality);
		} else {
			window = make_uniq<PhysicalWindow>(types, std::move(select_list), op.estimated_cardinality);
		}
		window->children.push_back(std::move(plan));
		plan = std::move(window);

		// Remember the projection order if we changed it
		if (!streaming_windows.empty() || !blocking_windows.empty() || !projection_map.empty()) {
			auto output_expr = output_pos;
			for (const auto &expr_idx : matching) {
				projection_map[input_width + expr_idx] = output_expr++;
			}
		}

		output_pos += matching.size();
	}

	// Put everything back into place if it moved
	if (!projection_map.empty()) {
		vector<unique_ptr<Expression>> select_list(op.types.size());
		// The inputs don't move
		for (idx_t i = 0; i < input_width; ++i) {
			select_list[i] = make_uniq<BoundReferenceExpression>(op.types[i], i);
		}
		// The outputs have been rearranged
		for (const auto &p : projection_map) {
			select_list[p.first] = make_uniq<BoundReferenceExpression>(op.types[p.first], p.second);
		}
		auto proj = make_uniq<PhysicalProjection>(op.types, std::move(select_list), op.estimated_cardinality);
		proj->children.push_back(std::move(plan));
		plan = std::move(proj);
	}

	return plan;
}

} // namespace duckdb
