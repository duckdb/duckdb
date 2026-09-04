#include "duckdb/optimizer/constraint_propagator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

//! Helper to find the projection index of a physical column in a LogicalGet
static optional_idx FindProjectionIndex(const LogicalGet &get, PhysicalIndex phys_key) {
	auto &column_ids = get.GetColumnIds();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i].GetPrimaryIndex() == phys_key.index) {
			return i;
		}
	}
	return optional_idx();
}

//! Map physical column indices to logical ColumnBindings
static column_binding_set_t MapPhysicalToLogical(const LogicalGet &get, const vector<PhysicalIndex> &phys_keys,
                                                 bool &all_found) {
	column_binding_set_t result;
	all_found = true;
	for (auto &phys_key : phys_keys) {
		auto idx = FindProjectionIndex(get, phys_key);
		if (!idx.IsValid()) {
			all_found = false;
			return {};
		}
		result.insert(ColumnBinding(get.table_index, ProjectionIndex(idx.GetIndex())));
	}
	return result;
}

//! Helper to get child table indices
static unordered_set<TableIndex> GetChildTableIndices(LogicalOperator &op) {
	unordered_set<TableIndex> indices;
	if (!op.children.empty()) {
		for (auto &b : op.children[0]->GetColumnBindings()) {
			indices.insert(b.table_index);
		}
	}
	return indices;
}

//! Helper to propagate base table and filter flag if there's only one child table
static void PropagateBaseTableAndFilter(const ConstraintProperties &child_props, size_t num_child_tables,
                                        ConstraintProperties &props) {
	if (num_child_tables == 1) {
		props.base_table = child_props.base_table;
		if (child_props.has_filter) {
			props.has_filter = true;
		}
	}
}

bool ConstraintProperties::IsUnique(const vector<ColumnBinding> &cols) const {
	if (cols.empty()) {
		return false;
	}

	TableIndex first_table = cols[0].table_index;
	column_binding_set_t col_set;
	for (auto &col : cols) {
		if (col.table_index != first_table) {
			return false;
		}
		col_set.insert(col);
	}

	for (auto &set : unique_sets) {
		if (set.size() > col_set.size()) {
			continue;
		}
		bool all_in = true;
		for (auto &c : set) {
			if (col_set.find(c) == col_set.end()) {
				all_in = false;
				break;
			}
		}
		if (all_in) {
			return true;
		}
	}
	return false;
}

bool ConstraintProperties::IsForeignKey(const vector<ColumnBinding> &cols, const Identifier &schema,
                                        const Identifier &table) const {
	if (cols.empty()) {
		return false;
	}

	TableIndex first_table = cols[0].table_index;
	column_binding_set_t col_set;
	for (auto &col : cols) {
		if (col.table_index != first_table) {
			return false;
		}
		col_set.insert(col); // Deduplicate
	}

	for (auto &fk : foreign_keys) {
		if (fk.referenced_schema != schema || fk.referenced_table != table) {
			continue;
		}
		if (fk.fk_columns.size() != col_set.size()) { // Use deduplicated size
			continue;
		}

		bool match = true;
		for (auto &col : col_set) {
			if (fk.fk_columns.find(col) == fk.fk_columns.end()) {
				match = false;
				break;
			}
		}
		if (match) {
			return true;
		}
	}
	return false;
}

bool ConstraintProperties::IsNotNull(const vector<ColumnBinding> &cols) const {
	if (cols.empty()) {
		return false;
	}
	for (auto &col : cols) {
		if (not_null_columns.find(col) == not_null_columns.end()) {
			return false;
		}
	}
	return true;
}

void ConstraintPropagator::VisitOperator(LogicalOperator &op) {
	for (auto &child : op.children) {
		VisitOperator(*child);
	}

	ConstraintProperties props;

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		auto table_ptr = get.GetTable();
		if (!table_ptr) {
			break;
		}

		auto &table = *table_ptr;
		auto &columns = table.GetColumns();

		props.base_table = &table;

		if (get.table_filters.HasFilters()) {
			props.has_filter = true;
		}

		for (auto &constraint : table.GetConstraints()) {
			if (constraint->type == ConstraintType::UNIQUE) {
				auto &unique = constraint->Cast<UniqueConstraint>();

				auto logical_indexes = unique.GetLogicalIndexes(columns);
				vector<PhysicalIndex> phys_keys;
				for (auto &log_idx : logical_indexes) {
					phys_keys.push_back(columns.GetColumn(log_idx).Physical());
				}

				bool all_found = false;
				auto unique_set = MapPhysicalToLogical(get, phys_keys, all_found);
				if (all_found) {
					props.unique_sets.push_back(std::move(unique_set));
				}
			} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
				auto &fk = constraint->Cast<ForeignKeyConstraint>();

				if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
				    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
					bool all_found = false;
					auto fk_set = MapPhysicalToLogical(get, fk.info.fk_keys, all_found);
					if (all_found) {
						Identifier fk_schema = fk.info.schema;
						if (fk_schema.empty()) {
							fk_schema = table.schema.name;
						}
						props.foreign_keys.emplace_back(fk_schema, fk.info.table, std::move(fk_set));
					}
				}
			} else if (constraint->type == ConstraintType::NOT_NULL) {
				auto &not_null = constraint->Cast<NotNullConstraint>();
				auto phys_idx = columns.GetColumn(not_null.index).Physical();

				auto idx = FindProjectionIndex(get, phys_idx);
				if (idx.IsValid()) {
					props.not_null_columns.insert(ColumnBinding(get.table_index, ProjectionIndex(idx.GetIndex())));
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &proj = op.Cast<LogicalProjection>();
		auto child_table_indices = GetChildTableIndices(op);

		// Optimization: Build a map of child bindings to projection indices for O(1) lookup
		column_binding_map_t<idx_t> binding_map;
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			auto &expr = proj.expressions[i];
			if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &ref = expr->Cast<BoundColumnRefExpression>();
				binding_map[ref.Binding()] = i;
			}
		}

		// Helper lambda to map a set using the precomputed map
		auto map_set = [&](const column_binding_set_t &source_set, column_binding_set_t &target_set) -> bool {
			for (auto &col : source_set) {
				auto it = binding_map.find(col);
				if (it == binding_map.end()) {
					return false;
				}
				target_set.insert(ColumnBinding(proj.table_index, ProjectionIndex(it->second)));
			}
			return true;
		};

		for (auto child_table_idx : child_table_indices) {
			auto child_props_it = properties_map.find(child_table_idx);
			if (child_props_it == properties_map.end()) {
				continue;
			}
			auto &child_props = child_props_it->second;

			PropagateBaseTableAndFilter(child_props, child_table_indices.size(), props);

			// Map unique sets through projection
			for (auto &unique_set : child_props.unique_sets) {
				column_binding_set_t new_set;
				if (map_set(unique_set, new_set)) {
					props.unique_sets.push_back(std::move(new_set));
				}
			}

			// Map foreign keys through projection
			for (auto &fk : child_props.foreign_keys) {
				column_binding_set_t new_fk_set;
				if (map_set(fk.fk_columns, new_fk_set)) {
					props.foreign_keys.emplace_back(fk.referenced_schema, fk.referenced_table, std::move(new_fk_set));
				}
			}

			// Map not null columns through projection
			for (auto &col : child_props.not_null_columns) {
				auto it = binding_map.find(col);
				if (it != binding_map.end()) {
					props.not_null_columns.insert(ColumnBinding(proj.table_index, ProjectionIndex(it->second)));
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto child_table_indices = GetChildTableIndices(op);

		props.has_filter = true;

		for (auto child_table_idx : child_table_indices) {
			auto child_props_it = properties_map.find(child_table_idx);
			if (child_props_it != properties_map.end()) {
				auto &child_props = child_props_it->second;
				props.unique_sets.insert(props.unique_sets.end(), child_props.unique_sets.begin(),
				                         child_props.unique_sets.end());
				props.foreign_keys.insert(props.foreign_keys.end(), child_props.foreign_keys.begin(),
				                          child_props.foreign_keys.end());
				props.not_null_columns.insert(child_props.not_null_columns.begin(), child_props.not_null_columns.end());

				PropagateBaseTableAndFilter(child_props, child_table_indices.size(), props);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		if (distinct.distinct_type != DistinctType::DISTINCT) {
			break;
		}
		column_binding_set_t unique_set;
		for (auto &target : distinct.distinct_targets) {
			if (target->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = target->Cast<BoundColumnRefExpression>();
				unique_set.insert(col_ref.Binding());
			}
		}
		if (!unique_set.empty()) {
			props.unique_sets.push_back(std::move(unique_set));
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op.Cast<LogicalAggregate>();
		if (aggr.grouping_sets.size() > 1) {
			break;
		}
		column_binding_set_t unique_set;
		for (idx_t i = 0; i < aggr.groups.size(); i++) {
			unique_set.insert(ColumnBinding(aggr.group_index, ProjectionIndex(i)));
		}
		if (!unique_set.empty()) {
			props.unique_sets.push_back(unique_set);
		}
		break;
	}
	default:
		break;
	}

	// Propagate has_filter and base_table up from single-child operators
	if (op.children.size() == 1) {
		auto child_bindings = op.children[0]->GetColumnBindings();
		unordered_set<TableIndex> child_table_indices;
		for (auto &b : child_bindings) {
			child_table_indices.insert(b.table_index);
		}
		// Propagate if the child outputs exactly one table_index
		if (child_table_indices.size() == 1) {
			auto child_props_it = properties_map.find(*child_table_indices.begin());
			if (child_props_it != properties_map.end()) {
				if (!props.has_filter) {
					props.has_filter = child_props_it->second.has_filter;
				}
				if (!props.base_table) {
					props.base_table = child_props_it->second.base_table;
				}
			}
		}
	}

	auto bindings = op.GetColumnBindings();
	if (!bindings.empty() && op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    op.type != LogicalOperatorType::LOGICAL_ANY_JOIN && op.type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
	    op.type != LogicalOperatorType::LOGICAL_DELIM_JOIN && op.type != LogicalOperatorType::LOGICAL_ASOF_JOIN &&
	    op.type != LogicalOperatorType::LOGICAL_EXPLAIN) {
		properties_map[bindings[0].table_index] = std::move(props);
	}
}

bool ConstraintPropagator::IsKeyUnique(const vector<ColumnBinding> &keys) {
	if (keys.empty()) {
		return false;
	}
	auto it = properties_map.find(keys[0].table_index);
	if (it == properties_map.end()) {
		return false;
	}
	return it->second.IsUnique(keys);
}

bool ConstraintPropagator::IsForeignKey(const vector<ColumnBinding> &join_keys, const Identifier &schema,
                                        const Identifier &referenced_table) {
	if (join_keys.empty()) {
		return false;
	}
	auto it = properties_map.find(join_keys[0].table_index);
	if (it == properties_map.end()) {
		return false;
	}
	return it->second.IsForeignKey(join_keys, schema, referenced_table);
}

bool ConstraintPropagator::IsNotNull(const vector<ColumnBinding> &join_keys) {
	if (join_keys.empty()) {
		return false;
	}
	auto it = properties_map.find(join_keys[0].table_index);
	if (it == properties_map.end()) {
		return false;
	}
	return it->second.IsNotNull(join_keys);
}

} // namespace duckdb
