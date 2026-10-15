#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

void CompressedMaterialization::CompressAggregate(unique_ptr<LogicalOperator> &op) {
	auto &aggregate = op->Cast<LogicalAggregate>();
	auto &groups = aggregate.groups;
	column_binding_set_t group_binding_set;
	for (const auto &group : groups) {
		if (group->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &colref = group->Cast<BoundColumnRefExpression>();
		if (group_binding_set.find(colref.Binding()) != group_binding_set.end()) {
			return; // Duplicate group - don't compress
		}
		group_binding_set.insert(colref.Binding());
	}
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
	vector<CompressedMaterializationType> materialization_types(groups.size(), CompressedMaterializationType::INVALID);
	vector<unique_ptr<BaseStatistics>> stored_group_stats;
	stored_group_stats.resize(groups.size());
	vector<pair<idx_t, unique_ptr<BaseStatistics>>> variant_groups;
	auto try_compress_group = [&](idx_t group_idx, Expression &group_expr, optional_ptr<BaseStatistics> stats) {
		if (!stats) {
			return false;
		}
		auto compress_expr = GetCompressExpression(group_expr.Copy(), *stats);
		if (!compress_expr) {
			return false;
		}
		materialization_types[group_idx] = compress_expr->materialization_type;
		stored_group_stats[group_idx] = stats->ToUnique();
		groups[group_idx] = std::move(compress_expr->expression);
		group_stats[group_idx] = std::move(compress_expr->stats);
		return true;
	};
	// A VARIANT group may be compressed to a nested type if that removes a first(v) of it
	if (aggregate.grouping_sets.size() <= 1 && aggregate.grouping_functions.empty()) {
		for (const auto &group : groups) {
			auto colref = TryGetVariantWrapperColumnRef(*group);
			if (!colref) {
				continue;
			}
			for (const auto &expr : aggregate.expressions) {
				if (IsRemovableFirstAggregate(expr->Cast<BoundAggregateExpression>(), *colref)) {
					nested_variant_bindings.insert(colref->Binding());
				}
			}
		}
	}
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group_expr = *groups[group_idx];
		if (group_expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			auto &colref = group_expr.Cast<BoundColumnRefExpression>();
			group_bindings[group_idx] = colref.Binding();
			continue; // Will be compressed generically
		}

		// Mark the bindings referenced by the non-colref expression so they won't be modified
		GetReferencedBindings(group_expr, referenced_bindings);

		// The non-colref expression won't be compressed generically, so try to compress it here
		if (try_compress_group(group_idx, group_expr, GetVariantWrapperStats(group_expr))) {
			variant_groups.emplace_back(group_idx, stored_group_stats[group_idx]->ToUnique());
			continue;
		}
		try_compress_group(group_idx, group_expr, group_stats[group_idx].get());
	}

	// Anything referenced in the aggregate functions is also excluded
	for (idx_t expr_idx = 0; expr_idx < aggregate.expressions.size(); expr_idx++) {
		const auto &expr = *aggregate.expressions[expr_idx];
		D_ASSERT(expr.GetExpressionType() == ExpressionType::BOUND_AGGREGATE);
		const auto &aggr_expr = expr.Cast<BoundAggregateExpression>();
		for (const auto &child : aggr_expr.GetChildren()) {
			GetReferencedBindings(*child, referenced_bindings);
		}
		if (aggr_expr.GetFilter()) {
			GetReferencedBindings(*aggr_expr.GetFilter(), referenced_bindings);
		}
		if (aggr_expr.GetOrderBys()) {
			for (const auto &order : aggr_expr.GetOrderBys()->orders) {
				const auto &order_expr = *order.expression;
				if (order_expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
					GetReferencedBindings(order_expr, referenced_bindings);
				}
			}
		}
	}

	// Create info for compression
	CompressedMaterializationInfo info(*op, {0}, referenced_bindings);

	// Columns that the aggregate does not read cannot be referenced above it, so leave them uncompressed
	column_binding_set_t used_bindings;
	for (const auto &group : groups) {
		GetReferencedBindings(*group, used_bindings);
	}
	for (const auto &expr : aggregate.expressions) {
		GetReferencedBindings(*expr, used_bindings);
	}
	auto &child_info = info.child_info[0];
	for (idx_t binding_idx = 0; binding_idx < child_info.bindings_before.size(); binding_idx++) {
		if (used_bindings.find(child_info.bindings_before[binding_idx]) == used_bindings.end()) {
			child_info.can_compress[binding_idx] = false;
		}
	}

	// Create binding mapping
	const auto bindings_out = aggregate.GetColumnBindings();
	const auto &types = aggregate.types;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		// Aggregate changes bindings as it has a table idx
		CMBindingInfo binding_info(bindings_out[group_idx], types[group_idx]);
		if (!aggregate.grouping_sets.empty() && group_stats[group_idx]) {
			binding_info.stats = group_stats[group_idx]->ToUnique();
		}
		binding_info.materialization_type = materialization_types[group_idx];
		if (materialization_types[group_idx] != CompressedMaterializationType::INVALID) {
			// Compressed non-generically
			auto entry = info.binding_map.emplace(bindings_out[group_idx], std::move(binding_info));
			entry.first->second.stats = std::move(stored_group_stats[group_idx]);
		} else if (group_bindings[group_idx] != ColumnBinding()) {
			info.binding_map.emplace(group_bindings[group_idx], std::move(binding_info));
		}
	}

	// Now try to compress
	CreateProjections(op, info);
	nested_variant_bindings.clear();
	RemoveVariantFirstAggregates(op, variant_groups);

	// Update aggregate statistics
	UpdateAggregateStats(op);
}

bool CompressedMaterialization::IsRemovableFirstAggregate(const BoundAggregateExpression &aggr,
                                                          const BoundColumnRefExpression &variant_colref) {
	return aggr.Function().GetName() == FirstFun::Name && !aggr.IsDistinct() && !aggr.GetFilter() &&
	       !aggr.GetOrderBys() && aggr.StateExportMode() == AggregateStateExportMode::NONE &&
	       aggr.GetChildren().size() == 1 && aggr.GetChildren()[0]->Equals(variant_colref);
}

void CompressedMaterialization::RemoveVariantFirstAggregates(
    unique_ptr<LogicalOperator> &op, vector<pair<idx_t, unique_ptr<BaseStatistics>>> &variant_groups) {
	if (variant_groups.empty() || op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &projection = op->Cast<LogicalProjection>();
	auto &aggregate = op->children[0]->Cast<LogicalAggregate>();
	if (aggregate.grouping_sets.size() > 1 || !aggregate.grouping_functions.empty()) {
		// With multiple grouping sets the group can be NULL while first(v) is not
		return;
	}
	const auto group_count = aggregate.groups.size();
	D_ASSERT(projection.expressions.size() == group_count + aggregate.expressions.size());
	// The projection keeps its columns, so track which projection column each remaining aggregate restores
	vector<idx_t> aggregate_projection_idx;
	for (idx_t aggr_idx = 0; aggr_idx < aggregate.expressions.size(); aggr_idx++) {
		aggregate_projection_idx.push_back(group_count + aggr_idx);
	}
	for (auto &variant_group : variant_groups) {
		const auto group_idx = variant_group.first;
		// The VARIANT column the group was compressed from
		optional_ptr<const BoundColumnRefExpression> variant_colref;
		idx_t colref_count = 0;
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(*aggregate.groups[group_idx],
		                                                              [&](const BoundColumnRefExpression &colref) {
			                                                              variant_colref = &colref;
			                                                              colref_count++;
		                                                              });
		if (colref_count != 1 || variant_colref->GetReturnType().id() != LogicalTypeId::VARIANT) {
			continue;
		}
		for (idx_t aggr_idx = aggregate.expressions.size(); aggr_idx-- > 0;) {
			auto &aggr = aggregate.expressions[aggr_idx]->Cast<BoundAggregateExpression>();
			if (!IsRemovableFirstAggregate(aggr, *variant_colref)) {
				continue;
			}
			auto &restore_expr = projection.expressions[aggregate_projection_idx[aggr_idx]];
			if (restore_expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			// All values within the group have the same VARIANT representation, so first(v) is the group itself
			auto group_ref =
			    make_uniq<BoundColumnRefExpression>(aggregate.groups[group_idx]->GetReturnType(),
			                                        ColumnBinding(aggregate.group_index, ProjectionIndex(group_idx)));
			restore_expr =
			    GetVariantDecompress(std::move(group_ref), restore_expr->GetReturnType(), *variant_group.second);
			aggregate_projection_idx.erase(aggregate_projection_idx.begin() + NumericCast<int64_t>(aggr_idx));
			aggregate.expressions.erase(aggregate.expressions.begin() + NumericCast<int64_t>(aggr_idx));
			for (auto &expr : projection.expressions) {
				ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
				    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
					    auto &binding = colref.BindingMutable();
					    if (binding.table_index == aggregate.aggregate_index && binding.column_index > aggr_idx) {
						    binding.column_index = ProjectionIndex(binding.column_index.GetIndex() - 1);
					    }
				    });
			}
		}
	}
	aggregate.ResolveOperatorTypes();
	projection.ResolveOperatorTypes();
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
		if (colref.GetReturnType() == group_stats[group_idx]->GetType()) {
			continue;
		}
		auto it = statistics_map.find(colref.Binding());
		if (it != statistics_map.end() && it->second) {
			group_stats[group_idx] = it->second->ToUnique();
		}
	}
}

} // namespace duckdb
