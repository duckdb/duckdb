#include "duckdb/optimizer/distinctness.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

bool Distinctness::IsDistinctOn(LogicalOperator &op, const column_binding_set_t &keys) {
	if (keys.empty()) {
		return false;
	}
	// a superset of a unique set is unique as well
	for (auto &unique_set : GetUniqueSets(op)) {
		bool covered = true;
		for (auto &binding : unique_set) {
			if (keys.find(binding) == keys.end()) {
				covered = false;
				break;
			}
		}
		if (covered) {
			return true;
		}
	}
	return false;
}

bool Distinctness::GetJoinEqualityKeys(const LogicalComparisonJoin &join, idx_t child_idx, column_binding_set_t &keys) {
	keys.clear();
	for (auto &cond : join.conditions) {
		// other conditions only filter the matches
		if (!cond.IsComparison()) {
			continue;
		}
		if (cond.GetComparisonType() != ExpressionType::COMPARE_EQUAL &&
		    cond.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			continue;
		}
		auto &key = child_idx == 0 ? cond.GetLHS() : cond.GetRHS();
		if (key.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			// e.g. a cast: several values of the column can be equal to the same value of the other side
			return false;
		}
		keys.insert(key.Cast<BoundColumnRefExpression>().Binding());
	}
	return !keys.empty();
}

bool Distinctness::IsDistinctOnJoinKeys(LogicalComparisonJoin &join, idx_t child_idx) {
	column_binding_set_t keys;
	if (!GetJoinEqualityKeys(join, child_idx, keys)) {
		return false;
	}
	return IsDistinctOn(*join.children[child_idx], keys);
}

vector<column_binding_set_t> Distinctness::ComputeJoinUniqueSets(LogicalComparisonJoin &join) {
	vector<column_binding_set_t> result;
	switch (join.join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
	case JoinType::SINGLE:
		// these emit every row of the left side at most once
		return GetUniqueSets(*join.children[0]);
	case JoinType::INNER:
		// a row that is joined to a side that is distinct on its join keys finds at most one match
		if (IsDistinctOnJoinKeys(join, 1)) {
			result = GetUniqueSets(*join.children[0]);
		}
		if (IsDistinctOnJoinKeys(join, 0)) {
			auto &right_sets = GetUniqueSets(*join.children[1]);
			result.insert(result.end(), right_sets.begin(), right_sets.end());
		}
		if (result.size() > MAX_UNIQUE_SETS) {
			// this is the only place where the number of unique sets grows: bound it, so that a chain of key joins
			// does not accumulate them. Dropping a unique set only loses a proof
			result.resize(MAX_UNIQUE_SETS);
		}
		return result;
	case JoinType::LEFT:
		if (IsDistinctOnJoinKeys(join, 1)) {
			result = GetUniqueSets(*join.children[0]);
		}
		return result;
	default:
		return result;
	}
}

const vector<column_binding_set_t> &Distinctness::GetUniqueSets(LogicalOperator &op) {
	auto entry = unique_sets.find(op);
	if (entry != unique_sets.end()) {
		return entry->second;
	}
	// every operator is computed once: its children are looked up in the cache
	return unique_sets.emplace(op, ComputeUniqueSets(op)).first->second;
}

vector<column_binding_set_t> Distinctness::ComputeScanUniqueSets(LogicalGet &get) {
	// the PRIMARY KEY / UNIQUE NOT NULL constraints of a scanned table, if all of their columns are scanned
	vector<column_binding_set_t> result;
	// Table-in/out functions need not preserve the multiplicity of their input keys.
	auto table = get.GetTable();
	if (!table || !table->IsDuckTable() || !get.children.empty() || get.GetColumnIds().empty()) {
		return result;
	}
	unordered_map<idx_t, ColumnBinding> column_bindings;
	for (auto &binding : get.GetColumnBindings()) {
		auto &column_index = get.GetColumnIndex(binding);
		if (!column_index.HasPrimaryIndex() || column_index.HasChildren() || column_index.IsVirtualColumn()) {
			continue;
		}
		column_bindings.emplace(column_index.GetPrimaryIndex(), binding);
	}
	auto &columns = table->GetColumns();
	auto &constraints = table->GetConstraints();
	unordered_set<idx_t> not_null_columns;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			not_null_columns.insert(constraint->Cast<NotNullConstraint>().index.index);
		}
	}
	for (auto &constraint : constraints) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (unique.IsDeferred()) {
			continue;
		}
		auto &names = unique.GetColumnNames();
		if (!unique.HasIndex() && !std::all_of(names.begin(), names.end(),
		                                       [&](const Identifier &name) { return columns.ColumnExists(name); })) {
			continue;
		}
		column_binding_set_t key;
		bool usable = true;
		for (auto &column : unique.GetLogicalIndexes(columns)) {
			auto entry = column_bindings.find(column.index);
			if (entry == column_bindings.end() ||
			    (!unique.IsPrimaryKey() && not_null_columns.find(column.index) == not_null_columns.end())) {
				usable = false;
				break;
			}
			key.insert(entry->second);
		}
		if (usable && !key.empty()) {
			result.push_back(std::move(key));
		}
	}
	return result;
}

vector<column_binding_set_t> Distinctness::ComputeAggregateUniqueSets(LogicalAggregate &aggr) {
	// the groups of the grouping set of an aggregate with a single grouping set
	//	TODO: use child FDs to simplify groupings.
	vector<column_binding_set_t> result;
	if (aggr.groups.empty() || aggr.grouping_sets.size() > 1 || !aggr.grouping_functions.empty()) {
		return result;
	}
	column_binding_set_t key;
	if (aggr.grouping_sets.empty()) {
		for (auto group_idx : ProjectionIndex::GetIndexes(aggr.groups.size())) {
			key.insert(ColumnBinding(aggr.group_index, group_idx));
		}
	} else {
		for (auto group_idx : aggr.grouping_sets[0]) {
			if (group_idx.GetIndex() >= aggr.groups.size()) {
				return result;
			}
			key.insert(ColumnBinding(aggr.group_index, group_idx));
		}
	}
	if (!key.empty()) {
		result.push_back(std::move(key));
	}
	return result;
}

vector<column_binding_set_t> Distinctness::ComputeProjectionUniqueSets(LogicalProjection &projection) {
	// only plain column references forward a unique set: other expressions (e.g. casts) are not injective
	vector<column_binding_set_t> result;
	auto &child_sets = GetUniqueSets(*projection.children[0]);
	if (child_sets.empty()) {
		return result;
	}
	column_binding_map_t<ColumnBinding> forwarded;
	for (auto expr_idx : ProjectionIndex::GetIndexes(projection.expressions.size())) {
		auto &expr = *projection.expressions[expr_idx];
		if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			forwarded.emplace(expr.Cast<BoundColumnRefExpression>().Binding(),
			                  ColumnBinding(projection.table_index, expr_idx));
		}
	}
	for (auto &child_set : child_sets) {
		column_binding_set_t key;
		bool complete = true;
		for (auto &binding : child_set) {
			auto entry = forwarded.find(binding);
			if (entry == forwarded.end()) {
				complete = false;
				break;
			}
			key.insert(entry->second);
		}
		if (complete) {
			result.push_back(std::move(key));
		}
	}
	return result;
}

vector<column_binding_set_t> Distinctness::ComputeUniqueSets(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		return ComputeScanUniqueSets(op.Cast<LogicalGet>());
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return ComputeAggregateUniqueSets(op.Cast<LogicalAggregate>());
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return ComputeProjectionUniqueSets(op.Cast<LogicalProjection>());
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
		// these remove rows or add columns, but never duplicate a row
		return GetUniqueSets(*op.children[0]);
	case LogicalOperatorType::LOGICAL_SAMPLE:
		if (propagation != DistinctnessPropagation::UNARY_OPERATORS_AND_JOINS) {
			break;
		}
		return GetUniqueSets(*op.children[0]);
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		if (propagation != DistinctnessPropagation::UNARY_OPERATORS_AND_JOINS) {
			break;
		}
		return ComputeJoinUniqueSets(op.Cast<LogicalComparisonJoin>());
	default:
		break;
	}
	// we don't know if the operator preserves the unique sets of its children
	return vector<column_binding_set_t>();
}

} // namespace duckdb
