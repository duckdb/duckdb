#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

static void PopulateBindingMap(CompressedMaterializationInfo &info, const vector<ColumnBinding> &bindings_out,
                               const vector<LogicalType> &types, LogicalOperator &op_in) {
	const auto bindings_in = op_in.GetColumnBindings();
	for (const auto &binding : bindings_in) {
		// Joins do not change bindings, input binding is output binding
		for (idx_t col_idx_out = 0; col_idx_out < bindings_out.size(); col_idx_out++) {
			const auto &binding_out = bindings_out[col_idx_out];
			if (binding_out == binding) {
				// This one is projected out, add it to map
				info.binding_map.emplace(binding, CMBindingInfo(binding_out, types[col_idx_out]));
			}
		}
	}
}

void CompressedMaterialization::CompressComparisonJoin(unique_ptr<LogicalOperator> &op) {
	auto &join = op->Cast<LogicalComparisonJoin>();
	if (join.join_type == JoinType::MARK) {
		// Tricky to get bindings right. RHS binding stays the same even though it changes type. Skip for now
		return;
	}

	auto &left_child = *join.children[0];
	auto &right_child = *join.children[1];

#ifndef DEBUG
	// In debug mode, we always apply compressed materialization to joins regardless of cardinalities,
	// so that it is well-tested. In release mode, we require build cardinality > JOIN_BUILD_CARDINALITY_THRESHOLD,
	// and we require join_cardinality / build_cardinality < JOIN_CARDINALITY_RATIO_THRESHOLD,
	// so that we don't end up doing many more decompressions than compressions and hurting performance
	const auto build_cardinality = right_child.has_estimated_cardinality ? right_child.estimated_cardinality
	                                                                     : right_child.EstimateCardinality(context);
	const auto join_cardinality =
	    join.has_estimated_cardinality ? join.estimated_cardinality : join.EstimateCardinality(context);
	const double ratio = static_cast<double>(join_cardinality) / static_cast<double>(build_cardinality);
	if (build_cardinality < JOIN_BUILD_CARDINALITY_THRESHOLD || ratio > JOIN_CARDINALITY_RATIO_THRESHOLD) {
		return;
	}
#endif

	// Find all bindings referenced by non-colref expressions in the conditions
	// These are excluded from compression by projection
	// But we can try to compress the expression directly
	column_binding_set_t probe_compress_bindings;
	column_binding_set_t referenced_bindings;
	for (const auto &condition : join.conditions) {
		if (join.conditions.size() == 1 && join.type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			// We only try to compress the join condition cols if there's one join condition
			// Else it gets messy with the stats if one column shows up in multiple conditions
			if (condition.left->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF &&
			    condition.right->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				// Both are bound column refs, see if both can be compressed generically to the same type
				auto &lhs_colref = condition.left->Cast<BoundColumnRefExpression>();
				auto &rhs_colref = condition.right->Cast<BoundColumnRefExpression>();
				auto lhs_it = statistics_map.find(lhs_colref.binding);
				auto rhs_it = statistics_map.find(rhs_colref.binding);
				if (lhs_it != statistics_map.end() && rhs_it != statistics_map.end() && lhs_it->second &&
				    rhs_it->second) {
					// For joins we need to compress both using the same statistics, otherwise comparisons don't work
					auto merged_stats = lhs_it->second->Copy();
					merged_stats.Merge(*rhs_it->second);

					// If one can be compressed, both can (same stats)
					auto compress_expr = GetCompressExpression(condition.left->Copy(), merged_stats);
					if (compress_expr) {
						D_ASSERT(GetCompressExpression(condition.right->Copy(), merged_stats));
						// This will be compressed generically, but we have to merge the stats
						lhs_it->second->Merge(merged_stats);
						rhs_it->second->Merge(merged_stats);
						probe_compress_bindings.insert(lhs_colref.binding);
						continue;
					}
				}
			}
		}
		GetReferencedBindings(*condition.left, referenced_bindings);
		GetReferencedBindings(*condition.right, referenced_bindings);
	}

	if (join.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		for (auto &dec : join.duplicate_eliminated_columns) {
			GetReferencedBindings(*dec, referenced_bindings);
		}
	}

	// We mark the probe-side bindings that do not show up in the join condition as "referenced"
	// We don't want to compress these because they are streamed
	const auto probe_bindings = left_child.GetColumnBindings();
	for (auto &binding : probe_bindings) {
		if (probe_compress_bindings.find(binding) == probe_compress_bindings.end()) {
			referenced_bindings.insert(binding);
		}
	}

	// Create info for compression
	CompressedMaterializationInfo info(*op, {0, 1}, referenced_bindings);

	const auto bindings_out = join.GetColumnBindings();
	const auto &types = join.types;
	PopulateBindingMap(info, bindings_out, types, left_child);
	PopulateBindingMap(info, bindings_out, types, right_child);

	// Now try to compress
	CreateProjections(op, info);

	// Update join statistics
	UpdateComparisonJoinStats(op);
}

void CompressedMaterialization::UpdateComparisonJoinStats(unique_ptr<LogicalOperator> &op) {
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}

	// Update join stats if compressed
	auto &compressed_join = op->children[0]->Cast<LogicalComparisonJoin>();
	if (compressed_join.join_stats.empty()) {
		return; // Nothing to update
	}

	for (idx_t condition_idx = 0; condition_idx < compressed_join.conditions.size(); condition_idx++) {
		auto &condition = compressed_join.conditions[condition_idx];
		if (condition.left->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
		    condition.right->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue; // We definitely didn't compress these, nothing changed
		}
		if (condition_idx * 2 >= compressed_join.join_stats.size()) {
			break;
		}

		auto &lhs_colref = condition.left->Cast<BoundColumnRefExpression>();
		auto &rhs_colref = condition.right->Cast<BoundColumnRefExpression>();
		auto &lhs_join_stats = compressed_join.join_stats[condition_idx * 2];
		auto &rhs_join_stats = compressed_join.join_stats[condition_idx * 2 + 1];
		auto lhs_it = statistics_map.find(lhs_colref.binding);
		auto rhs_it = statistics_map.find(rhs_colref.binding);
		if (lhs_it != statistics_map.end() && lhs_it->second) {
			lhs_join_stats = lhs_it->second->ToUnique();
		}
		if (rhs_it != statistics_map.end() && rhs_it->second) {
			rhs_join_stats = rhs_it->second->ToUnique();
		}
	}
}

} // namespace duckdb
