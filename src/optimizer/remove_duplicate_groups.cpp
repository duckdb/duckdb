#include "duckdb/optimizer/remove_duplicate_groups.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

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
}

void RemoveDuplicateGroups::VisitAggregate(LogicalAggregate &aggr) {
	if (!aggr.grouping_functions.empty()) {
		return;
	}

	auto &groups = aggr.groups;

	column_binding_map_t<idx_t> duplicate_map;
	vector<pair<idx_t, idx_t>> duplicates;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		const auto &group = groups[group_idx];
		if (group->type != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		const auto &colref = group->Cast<BoundColumnRefExpression>();
		const auto &binding = colref.binding;
		const auto it = duplicate_map.find(binding);
		if (it == duplicate_map.end()) {
			duplicate_map.emplace(binding, group_idx);
		} else {
			duplicates.emplace_back(it->second, group_idx);
		}
	}

	if (duplicates.empty()) {
		return;
	}

	// Sort duplicates by max duplicate group idx, because we want to remove groups from the back
	sort(duplicates.begin(), duplicates.end(),
	     [](const pair<idx_t, idx_t> &lhs, const pair<idx_t, idx_t> &rhs) { return lhs.second > rhs.second; });

	// Now we want to remove the duplicates, but this alters the column bindings coming out of the aggregate,
	// so we keep track of how they shift and do another round of column binding replacements
	column_binding_map_t<ColumnBinding> group_binding_map;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		group_binding_map.emplace(ColumnBinding(aggr.group_index, group_idx),
		                          ColumnBinding(aggr.group_index, group_idx));
	}

	for (idx_t duplicate_idx = 0; duplicate_idx < duplicates.size(); duplicate_idx++) {
		const auto &duplicate = duplicates[duplicate_idx];
		const auto &remaining_idx = duplicate.first;
		const auto &removed_idx = duplicate.second;

		// Store expression and remove it from groups
		stored_expressions.emplace_back(std::move(groups[removed_idx]));
		groups.erase_at(removed_idx);

		// This optimizer should run before statistics propagation, so this should be empty
		// If it runs after, then group_stats should be updated too
		D_ASSERT(aggr.group_stats.empty());

		// Remove from grouping sets too
		for (auto &grouping_set : aggr.grouping_sets) {
			// Replace removed group with duplicate remaining group
			if (grouping_set.erase(removed_idx) != 0) {
				grouping_set.insert(remaining_idx);
			}

			// Indices shifted: Reinsert groups in the set with group_idx - 1
			vector<idx_t> group_indices_to_reinsert;
			for (auto &entry : grouping_set) {
				if (entry > removed_idx) {
					group_indices_to_reinsert.emplace_back(entry);
				}
			}
			for (const auto group_idx : group_indices_to_reinsert) {
				grouping_set.erase(group_idx);
			}
			for (const auto group_idx : group_indices_to_reinsert) {
				grouping_set.insert(group_idx - 1);
			}
		}

		// Update mapping
		auto it = group_binding_map.find(ColumnBinding(aggr.group_index, removed_idx));
		D_ASSERT(it != group_binding_map.end());
		it->second.column_index = remaining_idx;

		for (auto &map_entry : group_binding_map) {
			auto &new_binding = map_entry.second;
			if (new_binding.column_index > removed_idx) {
				new_binding.column_index--;
			}
		}
	}

	// Replace all references to the old group binding with the new group binding
	for (const auto &map_entry : group_binding_map) {
		auto it = column_references.find(map_entry.first);
		if (it != column_references.end()) {
			for (auto expr : it->second) {
				expr.get().binding = map_entry.second;
			}
		}
	}
}

unique_ptr<Expression> RemoveDuplicateGroups::VisitReplace(BoundColumnRefExpression &expr,
                                                           unique_ptr<Expression> *expr_ptr) {
	// add a column reference
	column_references[expr.binding].push_back(expr);
	return nullptr;
}

} // namespace duckdb
