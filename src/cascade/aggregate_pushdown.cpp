#include "duckdb/cascade/aggregate_pushdown.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

namespace {

using PushdownBindings = vector<std::pair<ColumnBinding, ColumnBinding>>;

bool PushdownIsSideBinding(const vector<ColumnBinding> &side, const ColumnBinding &binding) {
	for (auto &candidate : side) {
		if (candidate == binding) {
			return true;
		}
	}
	return false;
}

//! True if the expression reads no column of the given side at all. An expression with no
//! column references qualifies, so a constant is on every side at once.
bool PushdownReadsOnly(const Expression &expr, const vector<ColumnBinding> &side) {
	bool only = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    if (!PushdownIsSideBinding(side, colref.Binding())) {
			    only = false;
		    }
	    });
	return only;
}

bool PushdownExpressionReads(const Expression &expr, const vector<ColumnBinding> &side) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    if (PushdownIsSideBinding(side, colref.Binding())) {
			    found = true;
		    }
	    });
	return found;
}

//! The position of the grouping expression that *is* exactly this column, or nothing. A
//! predicate column that is only a function of a grouping column cannot be used: the
//! grouping column determines it, not the other way round.
optional_idx PushdownGroupingPosition(const LogicalAggregate &aggregate, const ColumnBinding &binding) {
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		auto &group = *aggregate.groups[i];
		if (group.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		if (group.Cast<BoundColumnRefExpression>().Binding() == binding) {
			return i;
		}
	}
	return optional_idx();
}

bool PushdownIsPlainColumnOf(const Expression &expr, const vector<ColumnBinding> &side, ColumnBinding &binding) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	if (PushdownIsSideBinding(side, colref.Binding())) {
		binding = colref.Binding();
		return true;
	}
	return false;
}

//! Whether every column this expression reads of the aggregated side is a plain grouping
//! column of the GroupBy that is being pushed down - i.e. whether the expression can still
//! be evaluated once that side has been aggregated. Columns of the other side are always
//! fine, because the join still carries it.
bool PushdownEvaluableAbove(const Expression &expr, const vector<ColumnBinding> &aggregated_bindings,
                            const LogicalAggregate &aggregate) {
	bool evaluable = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    if (!PushdownIsSideBinding(aggregated_bindings, colref.Binding())) {
			    return;
		    }
		    if (!PushdownGroupingPosition(aggregate, colref.Binding()).IsValid()) {
			    evaluable = false;
		    }
	    });
	return evaluable;
}

//! The base-table scan a side reduces to, skipping filters, or nothing.
optional_ptr<LogicalOperator> PushdownBaseTable(LogicalOperator &op) {
	auto current = &op;
	while (current->type == LogicalOperatorType::LOGICAL_FILTER && current->children.size() == 1) {
		current = current->children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	return current;
}

//! Section 3.1's second condition, in the form the catalog can answer: the join picks at
//! most one row of `kept` per group. That holds when a unique constraint of the kept table
//! is fully equated by the predicate to expressions the group determines - every equality
//! has the constraint's column on one side and an expression that does not read the kept
//! side on the other, and with the constraint's columns being grouping columns those
//! expressions are functions of the group. Without it the join above would multiply the
//! pre-aggregated group, and the two plans would count different rows.
bool PushdownKeyIsGuarded(LogicalComparisonJoin &join, LogicalOperator &kept, const vector<ColumnBinding> &kept_bindings,
                          const LogicalAggregate &aggregate) {
	auto base = PushdownBaseTable(kept);
	if (!base) {
		return false;
	}
	auto &get = base->Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table) {
		return false;
	}
	auto &column_list = table->GetColumns();
	auto &scan_columns = get.GetColumnIds();
	// The scan reads more columns than it exposes (filters are pushed into it and the rest is
	// projected away), and an exposed binding's column index is its position in the scan's
	// column list - the two lists are not parallel.
	auto exposed_position = [&](idx_t logical_index) {
		for (idx_t position = 0; position < kept_bindings.size(); position++) {
			auto scan_position = kept_bindings[position].column_index.GetIndexUnsafe();
			if (scan_position >= scan_columns.size()) {
				continue;
			}
			auto &column = scan_columns[scan_position];
			if (column.HasPrimaryIndex() && column.ToLogical().index == logical_index) {
				return position;
			}
		}
		return DConstants::INVALID_INDEX;
	};
	// The key candidates: every unique constraint of the table, plus the columns the
	// DUCKDB_CASCADE_KEYS experiment names (the benchmark schemas declare none, see
	// CascadeConfig::IsDeclaredKey).
	vector<vector<ColumnBinding>> candidates;
	for (auto &constraint : table->GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto indexes = constraint->Cast<UniqueConstraint>().GetLogicalIndexes(column_list);
		if (indexes.empty()) {
			continue;
		}
		vector<ColumnBinding> candidate;
		for (auto &index : indexes) {
			auto position = exposed_position(index.index);
			if (position == DConstants::INVALID_INDEX) {
				break;
			}
			candidate.push_back(kept_bindings[position]);
		}
		if (candidate.size() == indexes.size()) {
			candidates.push_back(std::move(candidate));
		}
	}
	auto &columns = table->GetColumns();
	for (idx_t position = 0; position < kept_bindings.size(); position++) {
		auto scan_position = kept_bindings[position].column_index.GetIndexUnsafe();
		if (scan_position >= scan_columns.size() || !scan_columns[scan_position].HasPrimaryIndex()) {
			continue;
		}
		auto logical = scan_columns[scan_position].ToLogical();
		if (logical.index >= columns.LogicalColumnCount()) {
			continue;
		}
		auto &column = columns.GetColumn(LogicalIndex(logical.index));
		if (CascadeConfig::IsDeclaredKey(table->name.GetIdentifierName(), column.Name().GetIdentifierName())) {
			candidates.push_back(vector<ColumnBinding> {kept_bindings[position]});
		}
	}
	for (auto &key_bindings : candidates) {
		bool all_grouping = true;
		for (auto &binding : key_bindings) {
			if (!PushdownGroupingPosition(aggregate, binding).IsValid()) {
				all_grouping = false;
				break;
			}
		}
		if (!all_grouping) {
			continue;
		}
		// Every one of those columns has to be equated to something that does not read the
		// kept side.
		bool covered = true;
		for (auto &binding : key_bindings) {
			bool equated = false;
			for (auto &condition : join.conditions) {
				if (!condition.IsComparison() ||
				    condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
					continue;
				}
				auto matches = [&](const Expression &key, const Expression &other) {
					if (key.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
						return false;
					}
					if (key.Cast<BoundColumnRefExpression>().Binding() != binding) {
						return false;
					}
					return !PushdownExpressionReads(other, kept_bindings);
				};
				if (matches(condition.GetLHS(), condition.GetRHS()) ||
				    matches(condition.GetRHS(), condition.GetLHS())) {
					equated = true;
					break;
				}
			}
			if (!equated) {
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

void PushdownRemapBindings(unique_ptr<Expression> &expr, const PushdownBindings &map) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
		    for (auto &entry : map) {
			    if (colref.Binding() == entry.first) {
				    colref.BindingMutable() = entry.second;
				    return;
			    }
		    }
	    });
}

void PushdownRewriteOperatorBindings(LogicalOperator &op, const PushdownBindings &map) {
	for (auto &expr : op.expressions) {
		PushdownRemapBindings(expr, map);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			PushdownRemapBindings(condition.LeftReference(), map);
			PushdownRemapBindings(condition.RightReference(), map);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition) {
			PushdownRemapBindings(condition, map);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		for (auto &group : op.Cast<LogicalAggregate>().groups) {
			PushdownRemapBindings(group, map);
		}
		break;
	}
	default:
		break;
	}
	// Operators that keep the expressions naming their child's columns in members of their
	// own and hand those columns on unchanged.
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		for (auto &order : op.Cast<LogicalOrder>().orders) {
			PushdownRemapBindings(order.expression, map);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		for (auto &order : op.Cast<LogicalTopN>().orders) {
			PushdownRemapBindings(order.expression, map);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		for (auto &target : distinct.distinct_targets) {
			PushdownRemapBindings(target, map);
		}
		if (distinct.order_by) {
			for (auto &order : distinct.order_by->orders) {
				PushdownRemapBindings(order.expression, map);
			}
		}
		break;
	}
	default:
		break;
	}
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		for (auto &column : op.Cast<LogicalComparisonJoin>().duplicate_eliminated_columns) {
			PushdownRemapBindings(column, map);
		}
	}
}

bool PushdownPassesBindingsThrough(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return true;
	default:
		return false;
	}
}

} // namespace

AggregatePushdown::AggregatePushdown(Binder &binder_p, ClientContext &context_p)
    : binder(binder_p), context(context_p) {
}

unique_ptr<LogicalOperator> AggregatePushdown::PushNode(
    unique_ptr<LogicalOperator> op, vector<std::pair<ColumnBinding, ColumnBinding>> &exports) {
	// A materialized CTE is DuckDB's common-subplan sharing: one definition, several
	// references that read it by position. A rewrite inside the definition would have to be
	// reflected in every reference's column list, which this pass does not do - and the
	// optimizer's `__common_subplan_*` nodes are exactly where that used to crash
	// (`Failed to bind column reference ...`, TPC-DS q65). So a CTE is a barrier.
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
	    op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		return op;
	}
	for (auto &child : op->children) {
		PushdownBindings child_exports;
		child = PushNode(std::move(child), child_exports);
		if (child_exports.empty()) {
			continue;
		}
		PushdownRewriteOperatorBindings(*op, child_exports);
		if (PushdownPassesBindingsThrough(*op)) {
			for (auto &entry : child_exports) {
				exports.push_back(entry);
			}
		}
	}
	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return op;
	}
	auto &aggregate = op->Cast<LogicalAggregate>();
	if (aggregate.children.size() != 1) {
		return op;
	}
	// Without a grouping there is nothing that pins a row of the other side down, and an
	// ungrouped aggregate returns a row even for empty input where the join cannot.
	if (aggregate.groups.empty()) {
		return op;
	}
	// Grouping sets enumerate their own members by position, and this rewrite takes the
	// aggregated side's members away, so only the plain form is handled: one set that lists
	// every group (which the binder attaches to an ordinary GROUP BY) and nothing else. It
	// becomes the new GroupBy's single set, which is the empty list.
	if (aggregate.grouping_sets.size() > 1 ||
	    (!aggregate.grouping_sets.empty() && aggregate.grouping_sets[0].size() != aggregate.groups.size())) {
		return op;
	}
	// A selection between the aggregate and the join is not a reason to give up: it filters
	// the joined rows before they are grouped, and - because the predicate it may use is
	// constant within a group (see the condition below) - filtering the groups instead is
	// the same thing. The selections stay exactly where they are; only the aggregate moves.
	auto current = aggregate.children[0].get();
	vector<LogicalFilter *> selections;
	while (current->type == LogicalOperatorType::LOGICAL_FILTER && current->children.size() == 1) {
		selections.push_back(&current->Cast<LogicalFilter>());
		current = current->children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return op;
	}
	auto &join = current->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::INNER || join.children.size() != 2) {
		return op;
	}

	// Which side can the functions be computed on? All of them have to read only that
	// side's columns - the paper's third condition.
	idx_t aggregated_side = DConstants::INVALID_INDEX;
	for (idx_t candidate = 0; candidate < 2; candidate++) {
		auto bindings = join.children[candidate]->GetColumnBindings();
		bool usable = true;
		for (auto &expr : aggregate.expressions) {
			if (!PushdownReadsOnly(*expr, bindings)) {
				usable = false;
				break;
			}
		}
		if (usable) {
			aggregated_side = candidate;
			break;
		}
	}
	if (aggregated_side == DConstants::INVALID_INDEX) {
		return op;
	}
	auto &kept = *join.children[1 - aggregated_side];
	auto aggregated_bindings = join.children[aggregated_side]->GetColumnBindings();
	auto kept_bindings = kept.GetColumnBindings();
	auto &original_group_index = aggregate.group_index;
	auto &original_aggregate_index = aggregate.aggregate_index;

	// Everything is decided before a single node is touched, so that giving up leaves the
	// plan exactly as it was.
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			return op;
		}
	}
	if (!PushdownKeyIsGuarded(join, kept, kept_bindings, aggregate)) {
		return op;
	}
	// Every grouping expression has to be reproducible above the join: either it is computed
	// on the aggregated side, where the new GroupBy does it, or it is a plain column of the
	// other side, which the join still supplies. The grouping expressions that read the
	// aggregated side also have to cover every column of it the predicate uses, or the
	// predicate could not be evaluated on the groups (condition one).
	vector<idx_t> new_group_position(aggregate.groups.size(), DConstants::INVALID_INDEX);
	vector<ColumnBinding> supplied_binding(aggregate.groups.size());
	vector<const Expression *> new_groups;
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		auto &group = *aggregate.groups[i];
		if (PushdownReadsOnly(group, aggregated_bindings)) {
			new_group_position[i] = new_groups.size();
			new_groups.push_back(&group);
			continue;
		}
		ColumnBinding supplied;
		if (PushdownIsPlainColumnOf(group, kept_bindings, supplied)) {
			supplied_binding[i] = supplied;
			continue;
		}
		return op;
	}
	for (auto &condition : join.conditions) {
		if (!PushdownEvaluableAbove(condition.GetLHS(), aggregated_bindings, aggregate) ||
		    !PushdownEvaluableAbove(condition.GetRHS(), aggregated_bindings, aggregate)) {
			return op;
		}
	}
	for (auto *filter : selections) {
		for (auto &expr : filter->expressions) {
			if (!PushdownEvaluableAbove(*expr, aggregated_bindings, aggregate)) {
				return op;
			}
		}
	}

	// Build the new GroupBy and put it in the join's place.
	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> new_expressions;
	for (auto &expr : aggregate.expressions) {
		new_expressions.push_back(expr->Copy());
	}
	auto pushed = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(new_expressions));
	for (auto *group : new_groups) {
		pushed->groups.push_back(group->Copy());
	}
	// The aggregate's child is the chain of selections (or the join itself); that chain is
	// what replaces the aggregate, with the new GroupBy attached inside it.
	auto replacement = std::move(aggregate.children[0]);
	pushed->children.push_back(std::move(join.children[aggregated_side]));
	join.children[aggregated_side] = std::move(pushed);
	// The child that side just got exposes a different set of columns, so the projection map
	// built for the old one would read past its end (bug #11).
	if (aggregated_side == 0) {
		join.left_projection_map.clear();
	} else {
		join.right_projection_map.clear();
	}
	// The predicate used to read the aggregated relation's columns; those are the new
	// GroupBy's grouping columns now, so the predicate has to be repointed at them.
	PushdownBindings column_map;
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		if (new_group_position[i] == DConstants::INVALID_INDEX) {
			continue;
		}
		auto &group = *aggregate.groups[i];
		if (group.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		column_map.emplace_back(group.Cast<BoundColumnRefExpression>().Binding(),
		                        ColumnBinding(group_index, ProjectionIndex(new_group_position[i])));
	}
	for (auto &condition : join.conditions) {
		PushdownRemapBindings(condition.LeftReference(), column_map);
		PushdownRemapBindings(condition.RightReference(), column_map);
	}
	// The selections read the aggregated side's columns too, and those moved with it.
	for (auto *filter : selections) {
		for (auto &expr : filter->expressions) {
			PushdownRemapBindings(expr, column_map);
		}
	}

	// The aggregation above disappears: whoever read its grouping columns reads them from
	// the join's other side or from the new GroupBy, and whoever read its results reads the
	// new GroupBy's.
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		if (new_group_position[i] != DConstants::INVALID_INDEX) {
			exports.emplace_back(ColumnBinding(original_group_index, ProjectionIndex(i)),
			                     ColumnBinding(group_index, ProjectionIndex(new_group_position[i])));
		} else {
			exports.emplace_back(ColumnBinding(original_group_index, ProjectionIndex(i)), supplied_binding[i]);
		}
	}
	for (idx_t i = 0; i < aggregate.expressions.size(); i++) {
		exports.emplace_back(ColumnBinding(original_aggregate_index, ProjectionIndex(i)),
		                     ColumnBinding(aggregate_index, ProjectionIndex(i)));
	}
	return replacement;
}

unique_ptr<LogicalOperator> AggregatePushdown::Push(unique_ptr<LogicalOperator> plan) {
	vector<std::pair<ColumnBinding, ColumnBinding>> exports;
	auto result = PushNode(std::move(plan), exports);
	result->ResolveOperatorTypes();
	return result;
}

} // namespace duckdb
