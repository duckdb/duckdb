#include "duckdb/execution/operator/aggregate/physical_streaming_window.hpp"
#include "duckdb/execution/operator/aggregate/physical_window.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalWindow &op) {
	D_ASSERT(op.children.size() == 1);

	op.estimated_cardinality = op.EstimateCardinality(context);
	reference<PhysicalOperator> plan = CreatePlan(*op.children[0]);
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

	// Identify streaming windows and partitioned windows
	using Columns = vector<column_t>;
	const bool enable_optimizer = Settings::Get<EnableOptimizerSetting>(context);
	vector<idx_t> blocking_windows;
	vector<idx_t> streaming_windows;
	vector<idx_t> partitioned_windows;
	vector<Columns> partitioned_columns;
	for (idx_t expr_idx = 0; expr_idx < op.expressions.size(); expr_idx++) {
		auto &wexpr = op.expressions[expr_idx]->Cast<BoundWindowExpression>();
		Columns partition_columns;
		if (enable_optimizer && PhysicalStreamingWindow::IsStreamingFunction(context, wexpr)) {
			streaming_windows.push_back(expr_idx);
		} else if (!wexpr.Partitions().empty() &&
		           HasSingleValuePartitions(context, wexpr.Partitions(), plan, partition_columns)) {
			partitioned_windows.push_back(expr_idx);
		} else {
			blocking_windows.push_back(expr_idx);
		}
		//	Index these by expr_idx so we don't have to move them...
		partitioned_columns.emplace_back(std::move(partition_columns));
	}

	// 	Streaming takes priority over partitioning
	const bool has_streaming = !streaming_windows.empty();
	if (has_streaming) {
		for (auto &expr_idx : partitioned_windows) {
			blocking_windows.emplace_back(expr_idx);
		}
		partitioned_windows.clear();
	}

	//	Find the widest partitioning supported by the input
	if (!partitioned_windows.empty()) {
		vector<idx_t> remaining;
		auto widest = *std::max_element(partitioned_windows.begin(), partitioned_windows.end(),
		                                [&](const idx_t lhs, const idx_t rhs) {
			                                return partitioned_columns[lhs].size() < partitioned_columns[rhs].size();
		                                });
		for (auto &expr_idx : partitioned_windows) {
			if (partitioned_columns[expr_idx] == partitioned_columns[widest]) {
				remaining.emplace_back(expr_idx);
			} else {
				blocking_windows.emplace_back(expr_idx);
			}
		}
		remaining.swap(partitioned_windows);
	}

	//	Restore blocking function order
	std::sort(blocking_windows.begin(), blocking_windows.end());

	// Process the window functions by sharing the partition/order definitions
	unordered_map<idx_t, idx_t> projection_map;
	vector<vector<idx_t>> window_expressions;
	idx_t special_count = 0;
	auto output_pos = input_width;
	auto &special_windows = streaming_windows.empty() ? partitioned_windows : streaming_windows;
	while (!blocking_windows.empty() || !special_windows.empty()) {
		const bool process_blocking = special_windows.empty();
		auto &remaining = process_blocking ? blocking_windows : special_windows;
		special_count += process_blocking ? 0 : 1;

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

			// CSE Elimination: Search for a previous match (volatile expressions must not be deduplicated)
			bool cse = false;
			if (!wexpr.IsVolatile()) {
				for (idx_t i = 0; i < matching.size(); ++i) {
					const auto match_idx = matching[i];
					auto &match_expr = op.expressions[match_idx]->Cast<BoundWindowExpression>();
					if (wexpr.Equals(match_expr)) {
						projection_map[input_width + expr_idx] = output_pos + i;
						cse = true;
						break;
					}
				}
			}
			if (cse) {
				continue;
			}

			// Is there a common sort prefix?
			const auto prefix = over_expr.GetSharedOrders(wexpr);
			if (prefix != MinValue<idx_t>(over_expr.OrderBy().size(), wexpr.OrderBy().size())) {
				unprocessed.emplace_back(expr_idx);
				continue;
			}
			matching.emplace_back(expr_idx);

			// Switch to the longer prefix
			if (prefix < wexpr.OrderBy().size()) {
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
		if (i >= special_count) {
			auto &window = Make<PhysicalWindow>(types, std::move(select_list), op.estimated_cardinality);
			window.children.push_back(plan);
			plan = window;
		} else if (has_streaming) {
			auto &window = Make<PhysicalStreamingWindow>(types, std::move(select_list), op.estimated_cardinality);
			window.children.push_back(plan);
			plan = window;
		} else {
			const auto expr_idx = matching[0];
			auto &partitions = partitioned_columns[expr_idx];
			auto &window =
			    Make<PhysicalWindow>(types, std::move(select_list), op.estimated_cardinality, std::move(partitions));
			window.children.push_back(plan);
			plan = window;
		}
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
		auto &proj = Make<PhysicalProjection>(op.types, std::move(select_list), op.estimated_cardinality);
		proj.children.push_back(plan);
		plan = proj;
	}

	return plan;
}

} // namespace duckdb
