#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

void CompressedMaterialization::ComparisonJoinGetReferencedBindings(const JoinCondition &condition,
                                                                    vector<ColumnBinding> &referenced_bindings) {
	if (condition.left->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = condition.left->Cast<BoundColumnRefExpression>();
		referenced_bindings.emplace_back(colref.binding);
	} else {
		GetReferencedBindings(*condition.left, referenced_bindings);
	}
	if (condition.right->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = condition.right->Cast<BoundColumnRefExpression>();
		referenced_bindings.emplace_back(colref.binding);
	} else {
		GetReferencedBindings(*condition.right, referenced_bindings);
	}
}

static void PopulateBindingMap(CompressedMaterializationInfo &info, const vector<ColumnBinding> &bindings_out,
                               const vector<LogicalType> &types, LogicalOperator &op_in) {
	const auto rhs_bindings_in = op_in.GetColumnBindings();
	for (const auto &rhs_binding : rhs_bindings_in) {
		// Joins do not change bindings, input binding is output binding
		for (idx_t col_idx_out = 0; col_idx_out < bindings_out.size(); col_idx_out++) {
			const auto &binding_out = bindings_out[col_idx_out];
			if (binding_out == rhs_binding) {
				// This one is projected out, add it to map
				info.binding_map.emplace(rhs_binding, CMBindingInfo(binding_out, types[col_idx_out]));
			}
		}
	}
}

void CompressedMaterialization::CompressComparisonJoin(unique_ptr<LogicalOperator> &op) {
	auto &join = op->Cast<LogicalComparisonJoin>();

	// Find all bindings referenced by non-colref expressions in the conditions
	// These are excluded from compression by projection
	// But we can try to compress the expression directly
	vector<ColumnBinding> referenced_bindings;
	for (const auto &condition : join.conditions) {
		if (join.conditions.size() == 1) {
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
						continue;
					}
				}
			}
		}
		ComparisonJoinGetReferencedBindings(condition, referenced_bindings);
	}

	for (const auto &condition : join.conditions) {
		if (condition.left->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF &&
		    condition.right->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			// Both are bound column refs, see if both can be compressed generically to the same type
			auto &lhs_colref = condition.left->Cast<BoundColumnRefExpression>();
			auto &rhs_colref = condition.right->Cast<BoundColumnRefExpression>();
			auto lhs_it = statistics_map.find(lhs_colref.binding);
			auto rhs_it = statistics_map.find(rhs_colref.binding);
			if (lhs_it != statistics_map.end() && rhs_it != statistics_map.end() && lhs_it->second && rhs_it->second) {
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
					continue;
				}
			}
		}
		ComparisonJoinGetReferencedBindings(condition, referenced_bindings);
	}

	// Create info for compression
	CompressedMaterializationInfo info(*op, {0, 1}, referenced_bindings);

	const auto bindings_out = join.GetColumnBindings();
	const auto &types = join.types;
	PopulateBindingMap(info, bindings_out, types, *op->children[0]);
	PopulateBindingMap(info, bindings_out, types, *op->children[1]);

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

		auto &lhs_colref = condition.left->Cast<BoundColumnRefExpression>();
		auto &rhs_colref = condition.right->Cast<BoundColumnRefExpression>();
		auto &lhs_join_stats = compressed_join.join_stats[condition_idx * 2];
		auto &rhs_join_stats = compressed_join.join_stats[condition_idx * 2 + 1];
		if (lhs_colref.return_type == lhs_join_stats->GetType()) {
			continue; // Wasn't compressed
		}
		D_ASSERT(rhs_colref.return_type != rhs_join_stats->GetType());

		auto lhs_it = statistics_map.find(lhs_colref.binding);
		auto rhs_it = statistics_map.find(rhs_colref.binding);
		D_ASSERT(lhs_it != statistics_map.end() && rhs_it != statistics_map.end());
		lhs_join_stats = lhs_it->second->ToUnique();
		rhs_join_stats = rhs_it->second->ToUnique();
	}
}

} // namespace duckdb
