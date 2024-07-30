#include "duckdb/execution/operator/aggregate/physical_streaming_window.hpp"
#include "duckdb/execution/operator/aggregate/physical_window.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

#include <numeric>

namespace duckdb {

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
	const bool enable_optimizer = ClientConfig::GetConfig(context).enable_optimizer;
	vector<idx_t> blocking_windows;
	vector<idx_t> streaming_windows;
	for (idx_t expr_idx = 0; expr_idx < op.expressions.size(); expr_idx++) {
		if (enable_optimizer && PhysicalStreamingWindow::IsStreamingFunction(context, op.expressions[expr_idx])) {
			streaming_windows.push_back(expr_idx);
		} else {
			blocking_windows.push_back(expr_idx);
		}
	}

	// Process the window functions by sharing the partition/order definitions
	unordered_map<idx_t, idx_t> projection_map;
	vector<vector<idx_t>> window_expressions;
	idx_t blocking_count = 0;
	auto output_pos = input_width;
	while (!blocking_windows.empty() || !streaming_windows.empty()) {
		const bool process_streaming = blocking_windows.empty();
		auto &remaining = process_streaming ? streaming_windows : blocking_windows;
		blocking_count += process_streaming ? 0 : 1;

		// Find all functions that share the partitioning of the first remaining expression
		auto over_idx = remaining[0];

		vector<idx_t> matching;
		vector<idx_t> unprocessed;
		for (const auto &expr_idx : remaining) {
			D_ASSERT(op.expressions[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto &wexpr = op.expressions[expr_idx]->Cast<BoundWindowExpression>();

			// Just record the first one (it defines the partition)
			if (over_idx == expr_idx) {
				matching.emplace_back(expr_idx);
				continue;
			}

			// If it is in a different partition, skip it
			const auto &over_expr = op.expressions[over_idx]->Cast<BoundWindowExpression>();
			if (!over_expr.PartitionsAreEquivalent(wexpr)) {
				unprocessed.emplace_back(expr_idx);
				continue;
			}

			// CSE Elimination: Search for a previous match
			bool cse = false;
			for (idx_t i = 0; i < matching.size(); ++i) {
				const auto match_idx = matching[i];
				auto &match_expr = op.expressions[match_idx]->Cast<BoundWindowExpression>();
				if (wexpr.Equals(match_expr)) {
					projection_map[input_width + expr_idx] = output_pos + i;
					cse = true;
					break;
				}
			}
			if (cse) {
				continue;
			}

			// Is there a common sort prefix?
			const auto prefix = over_expr.GetSharedOrders(wexpr);
			if (prefix != MinValue<idx_t>(over_expr.orders.size(), wexpr.orders.size())) {
				unprocessed.emplace_back(expr_idx);
				continue;
			}
			matching.emplace_back(expr_idx);

			// Switch to the longer prefix
			if (prefix < wexpr.orders.size()) {
				over_idx = expr_idx;
			}
		}
		remaining.swap(unprocessed);

		// Remember the projection order
		for (const auto &expr_idx : matching) {
			projection_map[input_width + expr_idx] = output_pos++;
		}

		window_expressions.emplace_back(std::move(matching));
	}

	// Build the window operators
	for (idx_t i = 0; i < window_expressions.size(); ++i) {
		// Extract the matching expressions
		const auto &matching = window_expressions[i];
		vector<unique_ptr<Expression>> select_list;
		for (const auto &expr_idx : matching) {
			select_list.emplace_back(std::move(op.expressions[expr_idx]));
			types.emplace_back(op.types[input_width + expr_idx]);
		}

		// Chain the new window operator on top of the plan
		unique_ptr<PhysicalOperator> window;
		if (i < blocking_count) {
			window = make_uniq<PhysicalWindow>(types, std::move(select_list), op.estimated_cardinality);
		} else {
			window = make_uniq<PhysicalStreamingWindow>(types, std::move(select_list), op.estimated_cardinality);
		}
		window->children.push_back(std::move(plan));
		plan = std::move(window);
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
