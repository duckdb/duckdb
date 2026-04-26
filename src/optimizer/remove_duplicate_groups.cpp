#include "duckdb/optimizer/remove_duplicate_groups.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

RemoveDuplicateGroups::RemoveDuplicateGroups(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

namespace {

// Collect distinct BoundColumnRef bindings into `result`. Returns false (and stops walking)
// once a second distinct binding is seen — callers that need a singleton can bail immediately.
bool CollectColumnRefBindings(const Expression &expr, column_binding_set_t &result) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		result.insert(expr.Cast<BoundColumnRefExpression>().binding);
		return result.size() <= 1;
	}
	bool ok = true;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (ok && !CollectColumnRefBindings(child, result)) {
			ok = false;
		}
	});
	return ok;
}

// Erase `removed_idx` from aggr (groups + grouping_sets) and shift `group_binding_map`. Caller
// has already moved `aggr.groups[removed_idx]`'s expression out.
void EraseGroupAndShift(LogicalAggregate &aggr, ProjectionIndex removed_idx, ProjectionIndex target_idx,
                        column_binding_map_t<ColumnBinding> &group_binding_map) {
	aggr.groups.erase_at(removed_idx);

	// This optimizer should run before statistics propagation, so this should be empty
	// If it runs after, then group_stats should be updated too
	D_ASSERT(aggr.group_stats.empty());

	// Remove from grouping sets too
	for (auto &grouping_set : aggr.grouping_sets) {
		GroupingSet new_set;
		for (auto entry : grouping_set) {
			// Replace removed group with duplicate remaining group
			ProjectionIndex out = entry == removed_idx ? target_idx : entry;
			// Indices shifted: Reinsert groups in the set with group_idx - 1
			if (out > removed_idx) {
				out = ProjectionIndex(out - 1);
			}
			new_set.insert(out);
		}
		grouping_set = std::move(new_set);
	}

	// Update mapping
	auto it = group_binding_map.find(ColumnBinding(aggr.group_index, removed_idx));
	D_ASSERT(it != group_binding_map.end());
	it->second.column_index = target_idx;

	for (auto &map_entry : group_binding_map) {
		auto &new_binding = map_entry.second;
		if (new_binding.column_index > removed_idx) {
			new_binding.column_index = ProjectionIndex(new_binding.column_index - 1);
		}
	}
}

} // namespace

void RemoveDuplicateGroups::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		VisitAggregate(op.Cast<LogicalAggregate>());
		break;
	default:
		break;
	}
	LogicalOperatorVisitor::VisitOperatorExpressions(op);
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	// Inject any pending derived-collapse projections (the only place we own the unique_ptr slot).
	for (auto &child_slot : op.children) {
		if (child_slot->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			continue;
		}
		auto pp = pending_projections.find(&child_slot->Cast<LogicalAggregate>());
		if (pp == pending_projections.end()) {
			continue;
		}
		auto proj = std::move(pp->second);
		pending_projections.erase(pp);
		if (child_slot->has_estimated_cardinality) {
			proj->SetEstimatedCardinality(child_slot->estimated_cardinality);
		}
		proj->children.emplace_back(std::move(child_slot));
		proj->ResolveOperatorTypes();
		child_slot = std::move(proj);
	}
}

void RemoveDuplicateGroups::VisitAggregate(LogicalAggregate &aggr) {
	if (!aggr.grouping_functions.empty()) {
		return;
	}

	// If there are multiple grouping sets (ROLLUP/CUBE), we cannot remove duplicate groups
	// because the position of groups matters semantically in ROLLUP(col1, col2, col3),
	// even if col1 and col3 reference the same column binding (e.g., after join column replacement)
	if (aggr.grouping_sets.size() > 1) {
		return;
	}

	auto &groups = aggr.groups;

	// Duplicates collapse in place; derived rewrites recompute in a projection above the aggregate.
	// `derived_expr` discriminates: null → duplicate, non-null → derived. The base's source binding
	// is recovered on demand from the (still-intact) base group via group_binding_map.
	struct Removal {
		ProjectionIndex removed_idx;
		ProjectionIndex target_idx;
		unique_ptr<Expression> derived_expr;
	};
	vector<Removal> removals;
	bool any_derived = false;

	column_binding_map_t<ProjectionIndex> first_occurrence;
	for (auto group_idx : ProjectionIndex::GetIndexes(groups.size())) {
		const auto &group = groups[group_idx];
		if (group->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		const auto &binding = group->Cast<BoundColumnRefExpression>().binding;
		auto it = first_occurrence.find(binding);
		if (it == first_occurrence.end()) {
			first_occurrence.emplace(binding, group_idx);
		} else {
			removals.push_back({group_idx, it->second, nullptr});
		}
	}

	// Derived: a non-column-ref group whose entire colref-set is a single binding that matches a
	// surviving colref base. Walk each candidate's expression tree exactly once to collect its
	// colref-set, then a single hash lookup picks the base. The base must be a bare column-ref
	// (trivially injective in itself) — a non-colref base like floor(x) would need an injectivity
	// proof on every derived to avoid splitting partitions for many-to-one inputs.
	for (auto group_idx : ProjectionIndex::GetIndexes(groups.size())) {
		const auto &group = groups[group_idx];
		if (group->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		if (group->IsFoldable() || !group->IsConsistent()) {
			continue;
		}
		column_binding_set_t colrefs;
		if (!CollectColumnRefBindings(*group, colrefs) || colrefs.size() != 1) {
			continue;
		}
		auto base_it = first_occurrence.find(*colrefs.begin());
		if (base_it == first_occurrence.end()) {
			continue;
		}
		removals.push_back({group_idx, base_it->second, nullptr});
		any_derived = true;
	}

	if (removals.empty()) {
		return;
	}

	const idx_t num_original_groups = groups.size();
	const idx_t num_aggregate_outputs = aggr.expressions.size();
	const TableIndex original_group_index = aggr.group_index;
	const TableIndex original_aggregate_index = aggr.aggregate_index;

	// Now we want to remove the duplicates, but this alters the column bindings coming out of the aggregate,
	// so we keep track of how they shift and do another round of column binding replacements
	column_binding_map_t<ColumnBinding> group_binding_map;
	for (auto group_idx : ProjectionIndex::GetIndexes(num_original_groups)) {
		ColumnBinding b(original_group_index, group_idx);
		group_binding_map.emplace(b, b);
	}

	// Sort duplicates by max duplicate group idx, because we want to remove groups from the back
	sort(removals.begin(), removals.end(),
	     [](const Removal &a, const Removal &b) { return a.removed_idx > b.removed_idx; });

	for (auto &r : removals) {
		// Store expression and remove it from groups
		auto &expr = groups[r.removed_idx];
		if (expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			r.derived_expr = std::move(expr);
		} else {
			stored_expressions.emplace_back(std::move(expr));
		}
		EraseGroupAndShift(aggr, r.removed_idx, r.target_idx, group_binding_map);
	}

	column_binding_map_t<ColumnBinding> derived_remap;
	if (any_derived) {
		// Build a projection mirroring the aggregate's pre-removal output layout. Its child slot
		// is filled by VisitOperator post-recursion (we don't own the unique_ptr here).
		const auto new_proj_idx = optimizer.binder.GenerateTableIndex();

		// Initialise every projection slot as a passthrough column-ref into the aggregate's
		// post-shift bindings; in the same loop populate derived_remap so consumers route through
		// the projection.
		vector<unique_ptr<Expression>> select_list;
		select_list.reserve(num_original_groups + num_aggregate_outputs);
		for (auto orig : ProjectionIndex::GetIndexes(num_original_groups)) {
			auto post = group_binding_map.at(ColumnBinding(original_group_index, orig));
			select_list.emplace_back(make_uniq<BoundColumnRefExpression>(groups[post.column_index]->return_type, post));
			derived_remap.emplace(ColumnBinding(original_group_index, orig), ColumnBinding(new_proj_idx, orig));
		}
		for (auto k : ProjectionIndex::GetIndexes(num_aggregate_outputs)) {
			select_list.emplace_back(make_uniq<BoundColumnRefExpression>(aggr.expressions[k.GetIndex()]->return_type,
			                                                             ColumnBinding(original_aggregate_index, k)));
			derived_remap.emplace(ColumnBinding(original_aggregate_index, k),
			                      ColumnBinding(new_proj_idx, ProjectionIndex(num_original_groups + k.GetIndex())));
		}

		// Patch derived slots: replace the passthrough with the rewritten derived expression.
		// `groups` is in post-shift state; the surviving base group at base_post.column_index is
		// still a BoundColumnRefExpression carrying its source binding.
		ColumnBindingReplacer rebinder;
		for (auto &r : removals) {
			if (!r.derived_expr) {
				continue;
			}
			auto base_post = group_binding_map.at(ColumnBinding(original_group_index, r.target_idx));
			const auto &base_source = groups[base_post.column_index]->Cast<BoundColumnRefExpression>().binding;
			rebinder.replacement_bindings.clear();
			rebinder.replacement_bindings.emplace_back(base_source, base_post);
			rebinder.VisitExpression(&r.derived_expr);
			select_list[r.removed_idx.GetIndex()] = std::move(r.derived_expr);
		}

		pending_projections[&aggr] = make_uniq<LogicalProjection>(new_proj_idx, std::move(select_list));
	}

	// Replace all references to the old group binding with the new group binding
	for (const auto &[old_binding, new_binding] : any_derived ? derived_remap : group_binding_map) {
		ReplaceBinding(old_binding, new_binding);
	}
}

} // namespace duckdb
