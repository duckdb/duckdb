#include "duckdb/optimizer/remove_useless_groups.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

void RemoveUselessGroups::VisitOperator(LogicalOperator &op) {
	if (!finish_collection) {
		// We need to collect the primary key set at first, and we only need to collect once.
		CollectPrimaryKeySet(op);
		finish_collection = true;
		if (table_primary_key_map.empty()) {
			// There are no primary key, so we can return directly.
			return;
		}
	}

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

void RemoveUselessGroups::VisitAggregate(LogicalAggregate &aggr) {
	// TODO: Support more grouping sets
	if (!aggr.grouping_functions.empty() || aggr.grouping_sets.size() > 1) {
		return;
	}

	auto &groups = aggr.groups;

	// This optimization should be put after the `remove_duplicate_groups`,
	// so we need to some checks to guarantee this conditions.
	column_binding_set_t duplicate_set;

	// The map "table_used_columns_map" is utilized to track the appearance of columns in the current group by keys.
	// The keys in the map correspond to the table index of the columns, while the values consist of pairs.
	// - The "pair.first" value corresponds to the column index within the table.
	// - The "pair.second" value corresponds to the position of the column within the "groups".
	// This structure groups all columns from the same table that appear in the group by keys.
	// This information is used to determine whether the columns
	// from a single table within the group by keys cover the primary key.
	map<idx_t, vector<pair<idx_t, idx_t>>> table_used_columns_map;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		const auto &group = groups[group_idx];
		if (group->type != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		const auto &colref = group->Cast<BoundColumnRefExpression>();
		const auto &binding = colref.binding;
		// We should guarantee there are no same column bindings in the same table;
		const auto it = duplicate_set.find(binding);
		D_ASSERT(it == duplicate_set.end());
		duplicate_set.insert(binding);
		table_used_columns_map[binding.table_index].emplace_back(binding.column_index, group_idx);
	}

	// useless_group_index used to check whether the elements are useless_column_index ot not.
	// If `useless_group_index[i] = true`, it means the ith group by key is useless_column_index.
	vector<bool> useless_group_index(groups.size());
	bool has_useless_keys = false;
	// We need to iterate through the columns that appear in the GROUP BY clause based on their table index.
	// This allows us to determine if these columns can cover the primary key of the corresponding table.
	for (const auto &it : table_used_columns_map) {
		idx_t table_index = it.first;
		if (table_primary_key_map.find(table_index) == table_primary_key_map.end()) {
			continue;
		}
		vector<pair<idx_t, idx_t>> used_group_columns = it.second;
		auto primary_key_columns = table_primary_key_map[table_index];
		size_t count = 0;
		if (used_group_columns.size() < primary_key_columns.size()) {
			continue;
		}
		vector<idx_t> non_primary_key_columns;
		bool non_primary_key_used = true;
		for (idx_t idx = 0; idx < used_group_columns.size(); idx++) {
			auto column_index = used_group_columns[idx].first;
			auto group_by_key_index = used_group_columns[idx].second;
			if (primary_key_columns.find(column_index) == primary_key_columns.end()) {
				// If there are some non primary key has been used, we shouldn't remove it.
				if (column_references.find(ColumnBinding(aggr.group_index, group_by_key_index)) !=
				    column_references.end()) {
					non_primary_key_used = false;
					break;
				}
				non_primary_key_columns.emplace_back(group_by_key_index);
			} else {
				count++;
			}
		}

		// If all of the columns in the primary key have been covered and there are no non-primary key has been used,
		// we can remove the useless_column_index keys in the group by clause.
		// TODO: Keep the used non-primary-key and remove the unused non-primary-key only.
		if (count == primary_key_columns.size() && non_primary_key_used) {
			for (idx_t useless_column_idx : non_primary_key_columns) {
				useless_group_index[useless_column_idx] = true;
				has_useless_keys = true;
			}
		}
	}

	// If there are no useless_column_index keys, we can return directly.
	if (!has_useless_keys) {
		return;
	}

	vector<idx_t> useless_column_index;
	// Now we want to remove the useless columns, but this alters the column bindings coming out of the aggregate,
	// so we keep track of how they shift and do another round of column binding replacements
	column_binding_map_t<ColumnBinding> group_binding_map;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		if (useless_group_index[group_idx]) {
			useless_column_index.emplace_back(group_idx);
		} else {
			group_binding_map.emplace(ColumnBinding(aggr.group_index, group_idx),
			                          ColumnBinding(aggr.group_index, group_idx));
		}
	}
	// We want to remove groups from the back, so we let the bigger group index first.
	std::reverse(useless_column_index.begin(), useless_column_index.end());

	for (idx_t useless_idx = 0; useless_idx < useless_column_index.size(); useless_idx++) {
		const auto &removed_idx = useless_column_index[useless_idx];

		// Store expression and remove it from groups
		stored_expressions.emplace_back(std::move(groups[removed_idx]));
		groups.erase(groups.begin() + removed_idx);

		// This optimizer should run before statistics propagation, so this should be empty
		// If it runs after, then group_stats should be updated too
		D_ASSERT(aggr.group_stats.empty());

		// Remove from grouping sets too
		D_ASSERT(aggr.grouping_sets.size() == 1);
		for (auto &grouping_set : aggr.grouping_sets) {
			grouping_set.erase(removed_idx);

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

void RemoveUselessGroups::CollectPrimaryKeySet(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		for (auto &child : op.children) {
			CollectPrimaryKeySet(*child);
		}
		return;
	}
	auto &get = op.Cast<LogicalGet>();
	auto table = get.GetTable().get();
	if (!table) {
		return;
	}
	auto storage_info = table->GetStorageInfo(context);
	for (const auto &index : storage_info.index_info) {
		// Currently, we are only collecting the primary key information.
		// We are using a map to store the primary key details.
		// The keys in the map correspond to table index,
		// while the values store sets of columns that form the primary key.
		if (index.is_primary) {
			// The column index in the index information is not the same in the output of the get operator.
			// We need to do some adjust based on the column_ids in the get operator.
			size_t col_cnt = 0;
			for (idx_t column_index : index.column_set) {
				for (idx_t idx = 0; idx < get.column_ids.size(); idx++) {
					if (column_index == get.column_ids[idx]) {
						table_primary_key_map[get.table_index].insert(idx);
						col_cnt++;
						break;
					}
				}
			}
			// Not all of the columns in the primary key have been used.
			if (col_cnt < index.column_set.size()) {
				table_primary_key_map.erase(get.table_index);
			}
			break;
		}
	}
	return;
}

unique_ptr<Expression> RemoveUselessGroups::VisitReplace(BoundColumnRefExpression &expr,
                                                         unique_ptr<Expression> *expr_ptr) {
	// add a column reference
	column_references[expr.binding].push_back(expr);
	return nullptr;
}

} // namespace duckdb
