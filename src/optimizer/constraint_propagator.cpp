#include "duckdb/optimizer/constraint_propagator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

namespace duckdb {

//! Map physical column indices to logical ColumnBindings
static column_binding_set_t MapPhysicalToLogical(const LogicalGet &get, const vector<PhysicalIndex> &phys_keys,
                                                 bool &all_found) {
	column_binding_set_t result;
	all_found = true;
	auto &column_ids = get.GetColumnIds();

	for (auto &phys_key : phys_keys) {
		bool found = false;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i].GetPrimaryIndex() == phys_key.index) {
				result.insert(ColumnBinding(get.table_index, ProjectionIndex(i)));
				found = true;
				break;
			}
		}
		if (!found) {
			all_found = false;
			break;
		}
	}
	return result;
}

//! Map a set of ColumnBindings through a Projection
static bool MapBindings(const LogicalProjection &proj, const column_binding_set_t &source_set,
                        column_binding_set_t &target_set) {
	for (auto &col : source_set) {
		bool found = false;
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			auto &expr = proj.expressions[i];
			if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &ref = expr->Cast<BoundColumnRefExpression>();
				if (ref.Binding() == col) {
					target_set.insert(ColumnBinding(proj.table_index, ProjectionIndex(i)));
					found = true;
					break;
				}
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

bool ConstraintProperties::IsUnique(const vector<ColumnBinding> &cols) const {
	if (cols.empty()) {
		return false;
	}

	TableIndex first_table = cols[0].table_index;
	for (auto &col : cols) {
		if (col.table_index != first_table) {
			return false;
		}
	}
	for (auto &set : unique_sets) {
		if (set.size() != cols.size()) {
			continue;
		}
		bool match = true;
		for (auto &col : cols) {
			if (set.find(col) == set.end()) {
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

bool ConstraintProperties::IsForeignKey(const vector<ColumnBinding> &cols, const Identifier &referenced_table) const {
	if (cols.empty()) {
		return false;
	}

	TableIndex first_table = cols[0].table_index;
	for (auto &col : cols) {
		if (col.table_index != first_table) {
			return false;
		}
	}
	for (auto &fk : foreign_keys) {
		if (fk.referenced_table != referenced_table) {
			continue;
		}
		if (fk.fk_columns.size() != cols.size()) {
			continue;
		}

		bool match = true;
		for (auto &col : cols) {
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
						props.foreign_keys.emplace_back(fk.info.schema, fk.info.table, std::move(fk_set));
					}
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &proj = op.Cast<LogicalProjection>();
		if (op.children[0]->children.empty()) {
			break;
		}

		unordered_set<TableIndex> child_table_indices;
		for (auto &b : op.children[0]->GetColumnBindings()) {
			child_table_indices.insert(b.table_index);
		}

		for (auto child_table_idx : child_table_indices) {
			auto child_props_it = properties_map.find(child_table_idx);
			if (child_props_it == properties_map.end()) {
				continue;
			}
			auto &child_props = child_props_it->second;

			if (child_table_indices.size() == 1) {
				props.base_table = child_props.base_table;
			}

			// Map unique sets through projection
			for (auto &unique_set : child_props.unique_sets) {
				column_binding_set_t new_set;
				if (MapBindings(proj, unique_set, new_set)) {
					props.unique_sets.push_back(std::move(new_set));
				}
			}

			// Map foreign keys through projection
			for (auto &fk : child_props.foreign_keys) {
				column_binding_set_t new_fk_set;
				if (MapBindings(proj, fk.fk_columns, new_fk_set)) {
					props.foreign_keys.emplace_back(fk.referenced_schema, fk.referenced_table, std::move(new_fk_set));
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		unordered_set<TableIndex> child_table_indices;
		for (auto &b : op.children[0]->GetColumnBindings()) {
			child_table_indices.insert(b.table_index);
		}

		for (auto child_table_idx : child_table_indices) {
			auto child_props_it = properties_map.find(child_table_idx);
			if (child_props_it != properties_map.end()) {
				auto &child_props = child_props_it->second;
				props.unique_sets.insert(props.unique_sets.end(), child_props.unique_sets.begin(),
				                         child_props.unique_sets.end());
				props.foreign_keys.insert(props.foreign_keys.end(), child_props.foreign_keys.begin(),
				                          child_props.foreign_keys.end());
				if (child_table_indices.size() == 1) {
					props.base_table = child_props.base_table;
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op.Cast<LogicalAggregate>();
		// GROUP BY keys are guaranteed to be unique in the output!
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

	auto bindings = op.GetColumnBindings();
	if (!bindings.empty()) {
		properties_map[bindings[0].table_index] = std::move(props);
	}

	LogicalOperatorVisitor::VisitOperator(op);
}

bool ConstraintPropagator::IsJoinKeyUnique(const vector<ColumnBinding> &join_keys) {
	if (join_keys.empty()) {
		return false;
	}
	auto it = properties_map.find(join_keys[0].table_index);
	if (it == properties_map.end()) {
		return false;
	}
	return it->second.IsUnique(join_keys);
}

bool ConstraintPropagator::IsForeignKey(const vector<ColumnBinding> &join_keys, const Identifier &referenced_table) {
	if (join_keys.empty()) {
		return false;
	}
	auto it = properties_map.find(join_keys[0].table_index);
	if (it == properties_map.end()) {
		return false;
	}
	return it->second.IsForeignKey(join_keys, referenced_table);
}

} // namespace duckdb
