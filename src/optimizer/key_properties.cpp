#include "duckdb/optimizer/key_properties.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

static optional_idx FindBinding(const ColumnBinding &binding, LogicalOperator &input) {
	auto bindings = input.GetColumnBindings();
	for (idx_t index = 0; index < bindings.size(); index++) {
		if (bindings[index] == binding) {
			return optional_idx(index);
		}
	}
	return optional_idx();
}

static optional_idx GetKeyPropertyDirectReferenceIndex(const Expression &expression, LogicalOperator &input) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto index = expression.Cast<BoundReferenceExpression>().Index();
		return index < input.GetColumnBindings().size() ? optional_idx(index) : optional_idx();
	}
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return optional_idx();
	}
	return FindBinding(expression.Cast<BoundColumnRefExpression>().Binding(), input);
}

static bool TraceBaseColumns(LogicalOperator &op, vector<idx_t> &column_indices, optional_ptr<LogicalGet> &base_scan) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER: {
		if (op.children.size() != 1) {
			return false;
		}
		auto &filter = op.Cast<LogicalFilter>();
		if (!filter.projection_map.empty()) {
			for (auto &index : column_indices) {
				if (index >= filter.projection_map.size()) {
					return false;
				}
				index = filter.projection_map[index].GetIndex();
			}
		}
		return TraceBaseColumns(*op.children[0], column_indices, base_scan);
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (op.children.size() != 1) {
			return false;
		}
		auto &projection = op.Cast<LogicalProjection>();
		auto &child = *op.children[0];
		for (auto &index : column_indices) {
			if (index >= projection.expressions.size()) {
				return false;
			}
			auto child_index = GetKeyPropertyDirectReferenceIndex(*projection.expressions[index], child);
			if (!child_index.IsValid()) {
				return false;
			}
			index = child_index.GetIndex();
		}
		return TraceBaseColumns(child, column_indices, base_scan);
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (get.function.name != "seq_scan" || !get.GetTable()) {
			return false;
		}
		auto bindings = get.GetColumnBindings();
		for (auto &index : column_indices) {
			if (index >= bindings.size()) {
				return false;
			}
			auto &column_index = get.GetColumnIndex(bindings[index]);
			if (!column_index.HasPrimaryIndex() || column_index.HasChildren() || column_index.IsVirtualColumn()) {
				return false;
			}
			index = column_index.GetPrimaryIndex();
		}
		base_scan = get;
		return true;
	}
	default:
		return false;
	}
}

optional<UniqueKeyProperty> GetUniqueKeyProperty(LogicalOperator &owner, const vector<idx_t> &output_columns) {
	if (output_columns.empty()) {
		return nullopt;
	}
	auto logical_columns = output_columns;
	reference<LogicalOperator> current(owner);
	while (current.get().type == LogicalOperatorType::LOGICAL_FILTER ||
	       current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (current.get().children.size() != 1) {
			return nullopt;
		}
		if (current.get().type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = current.get().Cast<LogicalFilter>();
			if (!filter.projection_map.empty()) {
				for (auto &index : logical_columns) {
					if (index >= filter.projection_map.size()) {
						return nullopt;
					}
					index = filter.projection_map[index].GetIndex();
				}
			}
		} else {
			auto &projection = current.get().Cast<LogicalProjection>();
			for (auto &index : logical_columns) {
				if (index >= projection.expressions.size()) {
					return nullopt;
				}
				auto child_index =
				    GetKeyPropertyDirectReferenceIndex(*projection.expressions[index], *current.get().children[0]);
				if (!child_index.IsValid()) {
					return nullopt;
				}
				index = child_index.GetIndex();
			}
		}
		current = *current.get().children[0];
	}
	if (current.get().type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggregate = current.get().Cast<LogicalAggregate>();
		if (aggregate.groups.empty() || !aggregate.grouping_functions.empty() || aggregate.grouping_sets.size() > 1 ||
		    logical_columns.size() != aggregate.groups.size()) {
			return nullopt;
		}
		if (!aggregate.grouping_sets.empty()) {
			auto &grouping_set = aggregate.grouping_sets[0];
			if (grouping_set.size() != aggregate.groups.size()) {
				return nullopt;
			}
			for (idx_t group_idx = 0; group_idx < aggregate.groups.size(); group_idx++) {
				if (grouping_set.find(ProjectionIndex(group_idx)) == grouping_set.end()) {
					return nullopt;
				}
			}
		}
		vector<bool> matched_groups(aggregate.groups.size(), false);
		for (auto column : logical_columns) {
			if (column >= aggregate.groups.size() || matched_groups[column]) {
				return nullopt;
			}
			matched_groups[column] = true;
		}
		return UniqueKeyProperty {UniqueKeyProof::AGGREGATE_GROUP, nullptr};
	}
	if (current.get().type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = current.get().Cast<LogicalComparisonJoin>();
		if (join.children.size() != 2 || join.conditions.empty() || join.HasArbitraryConditions()) {
			return nullopt;
		}
		auto output_bindings = join.GetColumnBindings();
		idx_t key_child = DConstants::INVALID_INDEX;
		vector<idx_t> child_columns;
		for (auto column : logical_columns) {
			if (column >= output_bindings.size()) {
				return nullopt;
			}
			optional_idx child_column;
			for (idx_t child_idx = 0; child_idx < 2; child_idx++) {
				auto candidate = FindBinding(output_bindings[column], *join.children[child_idx]);
				if (!candidate.IsValid()) {
					continue;
				}
				if (key_child != DConstants::INVALID_INDEX && key_child != child_idx) {
					return nullopt;
				}
				key_child = child_idx;
				child_column = candidate;
				break;
			}
			if (!child_column.IsValid()) {
				return nullopt;
			}
			child_columns.push_back(child_column.GetIndex());
		}
		if (key_child == DConstants::INVALID_INDEX || !GetUniqueKeyProperty(*join.children[key_child], child_columns)) {
			return nullopt;
		}

		bool preserves_key = false;
		switch (join.join_type) {
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::MARK:
		case JoinType::SINGLE:
			preserves_key = key_child == 0;
			break;
		case JoinType::RIGHT_SEMI:
		case JoinType::RIGHT_ANTI:
			preserves_key = key_child == 1;
			break;
		case JoinType::INNER:
		case JoinType::LEFT:
		case JoinType::RIGHT: {
			if ((join.join_type == JoinType::LEFT && key_child != 0) ||
			    (join.join_type == JoinType::RIGHT && key_child != 1)) {
				break;
			}
			const auto other_child = 1 - key_child;
			vector<idx_t> other_columns;
			for (auto &condition : join.conditions) {
				if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL ||
				    condition.GetLHS().GetReturnType() != condition.GetRHS().GetReturnType()) {
					return nullopt;
				}
				auto left = GetKeyPropertyDirectReferenceIndex(condition.GetLHS(), *join.children[0]);
				auto right = GetKeyPropertyDirectReferenceIndex(condition.GetRHS(), *join.children[1]);
				if (!left.IsValid() || !right.IsValid()) {
					return nullopt;
				}
				other_columns.push_back(other_child == 0 ? left.GetIndex() : right.GetIndex());
			}
			preserves_key = GetUniqueKeyProperty(*join.children[other_child], other_columns).has_value();
			break;
		}
		default:
			break;
		}
		return preserves_key
		           ? optional<UniqueKeyProperty>(UniqueKeyProperty {UniqueKeyProof::KEY_PRESERVING_JOIN, nullptr})
		           : nullopt;
	}

	logical_columns = output_columns;
	optional_ptr<LogicalGet> base_scan;
	if (!TraceBaseColumns(owner, logical_columns, base_scan) || !base_scan) {
		return nullopt;
	}
	unordered_set<idx_t> key_set;
	for (auto column : logical_columns) {
		if (!key_set.insert(column).second) {
			return nullopt;
		}
	}
	auto &table = *base_scan->GetTable();
	unordered_set<idx_t> not_null_columns;
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			not_null_columns.insert(constraint->Cast<NotNullConstraint>().index.index);
		}
	}
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		auto indexes = unique.GetLogicalIndexes(table.GetColumns());
		if (indexes.size() != key_set.size()) {
			continue;
		}
		bool matches = true;
		for (auto index : indexes) {
			if (key_set.find(index.index) == key_set.end() ||
			    (!unique.IsPrimaryKey() && not_null_columns.find(index.index) == not_null_columns.end())) {
				matches = false;
				break;
			}
		}
		if (matches) {
			return UniqueKeyProperty {
			    unique.IsPrimaryKey() ? UniqueKeyProof::PRIMARY_KEY : UniqueKeyProof::UNIQUE_NOT_NULL, base_scan};
		}
	}
	return nullopt;
}

bool UniqueKeyProperty::FunctionallyDetermines(LogicalOperator &owner, idx_t output_column) const {
	if (proof == UniqueKeyProof::AGGREGATE_GROUP || proof == UniqueKeyProof::KEY_PRESERVING_JOIN) {
		return output_column < owner.GetColumnBindings().size();
	}
	vector<idx_t> columns {output_column};
	optional_ptr<LogicalGet> dependent_scan;
	return TraceBaseColumns(owner, columns, dependent_scan) && dependent_scan && dependent_scan == base_scan;
}

} // namespace duckdb
