#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

void CompressedMaterialization::CompressAggregate(unique_ptr<LogicalOperator> &op) {
	auto &aggregate = op->Cast<LogicalAggregate>();
	auto &groups = aggregate.groups;
	auto &group_stats = aggregate.group_stats;

	// No need to compress if there are no groups/stats
	if (groups.empty() || group_stats.empty()) {
		return;
	}
	D_ASSERT(groups.size() == group_stats.size());

	// Find all bindings referenced by non-colref expressions in the groups
	// These are excluded from compression by projection
	// But we can try to compress the expression directly
	column_binding_set_t referenced_bindings;
	vector<ColumnBinding> group_bindings(groups.size(), ColumnBinding());
	vector<bool> needs_decompression(groups.size(), false);
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group_expr = *groups[group_idx];
		if (group_expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			auto &colref = group_expr.Cast<BoundColumnRefExpression>();
			group_bindings[group_idx] = colref.binding;
			continue; // Will be compressed generically
		}

		// Mark the bindings referenced by the non-colref expression so they won't be modified
		GetReferencedBindings(group_expr, referenced_bindings);

		// The non-colref expression won't be compressed generically, so try to compress it here
		if (!group_stats[group_idx]) {
			continue; // Can't compress without stats
		}

		// Try to compress, if successful, replace the expression
		auto compress_expr = GetCompressExpression(group_expr.Copy(), *group_stats[group_idx]);
		if (compress_expr) {
			groups[group_idx] = std::move(compress_expr->expression);
			group_stats[group_idx] = std::move(compress_expr->stats);
			needs_decompression[group_idx] = true;
		}
	}

	// Anything referenced in the aggregate functions is also excluded
	for (idx_t expr_idx = 0; expr_idx < aggregate.expressions.size(); expr_idx++) {
		GetReferencedBindings(*aggregate.expressions[expr_idx], referenced_bindings);
	}

	// Create info for compression
	CompressedMaterializationInfo info(*op, {0}, referenced_bindings);

	// Create binding mapping
	const auto bindings_out = aggregate.GetColumnBindings();
	const auto &types = aggregate.types;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		// Aggregate changes bindings as it has a table idx
		CMBindingInfo binding_info(bindings_out[group_idx], types[group_idx]);
		binding_info.needs_decompression = needs_decompression[group_idx];
		if (needs_decompression[group_idx]) {
			// Compressed non-generically
			auto entry = info.binding_map.emplace(bindings_out[group_idx], std::move(binding_info));
			entry.first->second.stats = group_stats[group_idx]->ToUnique();
		} else if (group_bindings[group_idx] != ColumnBinding()) {
			info.binding_map.emplace(group_bindings[group_idx], std::move(binding_info));
		}
	}

	// Now try to compress
	CreateProjections(op, info);

	// Update aggregate statistics
	UpdateAggregateStats(op);
}

void CompressedMaterialization::UpdateAggregateStats(unique_ptr<LogicalOperator> &op) {
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}

	// Update aggregate group stats if compressed
	auto &compressed_aggregate = op->children[0]->Cast<LogicalAggregate>();
	auto &groups = compressed_aggregate.groups;
	auto &group_stats = compressed_aggregate.group_stats;

	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group_expr = *groups[group_idx];
		if (group_expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &colref = group_expr.Cast<BoundColumnRefExpression>();
		if (!group_stats[group_idx]) {
			continue;
		}
		if (colref.return_type == group_stats[group_idx]->GetType()) {
			continue;
		}
		auto it = statistics_map.find(colref.binding);
		D_ASSERT(it != statistics_map.end());
		group_stats[group_idx] = it->second->ToUnique();
	}
}

} // namespace duckdb
