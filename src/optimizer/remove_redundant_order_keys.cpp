#include "duckdb/optimizer/remove_redundant_order_keys.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

void FindUniqueColumnSets(LogicalGet &get, vector<column_binding_set_t> &result) {
	auto table = get.GetTable();
	if (!table || !table->IsDuckTable() || !get.projected_input.empty() || get.GetColumnIds().empty()) {
		return;
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
}

void FindUniqueColumnSets(LogicalAggregate &aggr, vector<column_binding_set_t> &result) {
	if (aggr.groups.empty() || aggr.grouping_sets.size() > 1 || !aggr.grouping_functions.empty()) {
		return;
	}
	column_binding_set_t key;
	if (aggr.grouping_sets.empty()) {
		for (auto group_idx : ProjectionIndex::GetIndexes(aggr.groups.size())) {
			key.insert(ColumnBinding(aggr.group_index, group_idx));
		}
	} else {
		for (auto group_idx : aggr.grouping_sets[0]) {
			if (group_idx.GetIndex() >= aggr.groups.size()) {
				return;
			}
			key.insert(ColumnBinding(aggr.group_index, group_idx));
		}
	}
	if (!key.empty()) {
		result.push_back(std::move(key));
	}
}

void FindUniqueColumnSets(LogicalDistinct &distinct, vector<column_binding_set_t> &result) {
	column_binding_set_t key;
	bool all_columns = true;
	for (auto &target : distinct.distinct_targets) {
		if (target->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			all_columns = false;
			break;
		}
		key.insert(target->Cast<BoundColumnRefExpression>().Binding());
	}
	if (all_columns && !key.empty()) {
		result.push_back(std::move(key));
	}
}

void ForwardUniqueColumnSets(LogicalProjection &projection, const vector<column_binding_set_t> &child_sets,
                             vector<column_binding_set_t> &result) {
	if (child_sets.empty()) {
		return;
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
}

void RemoveRedundantKeys(LogicalOrder &order, const vector<column_binding_set_t> &unique_sets) {
	if (order.orders.size() < 2 || unique_sets.empty()) {
		return;
	}
	column_binding_set_t prefix;
	for (idx_t i = 0; i + 1 < order.orders.size(); i++) {
		auto &expr = *order.orders[i].expression;
		if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		prefix.insert(expr.Cast<BoundColumnRefExpression>().Binding());
		for (auto &unique_set : unique_sets) {
			bool covered = true;
			for (auto &binding : unique_set) {
				if (prefix.find(binding) == prefix.end()) {
					covered = false;
					break;
				}
			}
			if (!covered) {
				continue;
			}
			order.orders.erase(order.orders.begin() + NumericCast<int64_t>(i + 1), order.orders.end());
			return;
		}
	}
}

void VisitOperator(LogicalOperator &op);

void VisitChildren(LogicalOperator &op, idx_t start) {
	for (idx_t child_idx = start; child_idx < op.children.size(); child_idx++) {
		VisitOperator(*op.children[child_idx]);
	}
}

vector<column_binding_set_t> CollectUniqueColumnSets(LogicalOperator &op) {
	vector<column_binding_set_t> result;
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = op.Cast<LogicalOrder>();
		result = CollectUniqueColumnSets(*order.children[0]);
		RemoveRedundantKeys(order, result);
		VisitChildren(op, 1);
		return result;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
		result = CollectUniqueColumnSets(*op.children[0]);
		VisitChildren(op, 1);
		return result;
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		auto child_sets = CollectUniqueColumnSets(*projection.children[0]);
		ForwardUniqueColumnSets(projection, child_sets, result);
		VisitChildren(op, 1);
		return result;
	}
	case LogicalOperatorType::LOGICAL_GET:
		FindUniqueColumnSets(op.Cast<LogicalGet>(), result);
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		FindUniqueColumnSets(op.Cast<LogicalAggregate>(), result);
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		FindUniqueColumnSets(op.Cast<LogicalDistinct>(), result);
		break;
	default:
		break;
	}
	VisitChildren(op, 0);
	return result;
}

void VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order = op.Cast<LogicalOrder>();
		if (order.orders.size() >= 2) {
			auto unique_sets = CollectUniqueColumnSets(*order.children[0]);
			RemoveRedundantKeys(order, unique_sets);
			VisitChildren(op, 1);
			return;
		}
	}
	VisitChildren(op, 0);
}

} // namespace

void RemoveRedundantOrderKeys::Optimize(LogicalOperator &op) {
	VisitOperator(op);
}

} // namespace duckdb
