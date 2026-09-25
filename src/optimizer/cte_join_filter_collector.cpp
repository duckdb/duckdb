#include "duckdb/optimizer/cte_join_filter_collector.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

CTEJoinFilterCollector::CTEJoinFilterCollector(const unordered_map<TableIndex, TableIndex> &targets)
    : join_targets(targets) {
}

vector<CTEJoinFilter> CTEJoinFilterCollector::Collect(LogicalOperator &op,
                                                      const unordered_map<TableIndex, TableIndex> &targets) {
	CTEJoinFilterCollector collector(targets);
	collector.VisitOperator(op);
	return std::move(collector.join_filters);
}

static bool CanPassCTEJoinFilter(LogicalOperator &op) {
	bool safe = true;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
		safe &= !(*expr)->IsVolatile() && !(*expr)->CanThrow() && !ExpressionBarrier::Contains(**expr);
		ExpressionIterator::VisitExpression<BoundFunctionExpression>(
		    **expr, [&](const BoundFunctionExpression &func) { safe &= !func.Function().RequiresOrderedExecution(); });
	});
	return safe;
}

static bool ContainsCTEJoinKeys(LogicalOperator &op, const vector<ColumnBinding> &keys) {
	auto bindings = op.GetColumnBindings();
	for (auto &key : keys) {
		if (std::find(bindings.begin(), bindings.end(), key) == bindings.end()) {
			return false;
		}
	}
	return true;
}

static bool MapCTEJoinProjection(LogicalProjection &projection, vector<ColumnBinding> &keys) {
	for (auto &key : keys) {
		if (key.table_index != projection.table_index) {
			return false;
		}
		auto &expr = projection.GetExpression(key);
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		key = expr.Cast<BoundColumnRefExpression>().Binding();
	}
	return true;
}

static optional_ptr<LogicalCTERef> FindCTEJoinSource(LogicalOperator &op, vector<ColumnBinding> &keys) {
	if (!ContainsCTEJoinKeys(op, keys)) {
		return nullptr;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		auto &ref = op.Cast<LogicalCTERef>();
		return ref.is_recurring ? nullptr : &ref;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION:
		if (!MapCTEJoinProjection(op.Cast<LogicalProjection>(), keys)) {
			return nullptr;
		}
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		break;
	default:
		return nullptr;
	}
	return FindCTEJoinSource(*op.children[0], keys);
}

void CTEJoinFilterCollector::VisitOperator(LogicalOperator &op) {
	for (auto &child : op.children) {
		VisitOperator(*child);
	}
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op.Cast<LogicalComparisonJoin>().join_type != JoinType::INNER || !CanPassCTEJoinFilter(op)) {
		return;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	vector<ColumnBinding> left_keys, right_keys;
	vector<ExpressionType> comparisons;
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() ||
		    (condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL &&
		     condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) ||
		    condition.GetLHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    condition.GetRHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		left_keys.push_back(condition.GetLHS().Cast<BoundColumnRefExpression>().Binding());
		right_keys.push_back(condition.GetRHS().Cast<BoundColumnRefExpression>().Binding());
		comparisons.push_back(condition.GetComparisonType());
	}
	if (comparisons.empty()) {
		return;
	}
	for (idx_t side = 0; side < 2; side++) {
		auto source_keys = side == 0 ? left_keys : right_keys;
		auto source = FindCTEJoinSource(*op.children[side], source_keys);
		if (source) {
			PushFilter(*op.children[1 - side], *source, source_keys, side == 0 ? right_keys : left_keys, comparisons);
		}
	}
}

void CTEJoinFilterCollector::PushFilter(LogicalOperator &op, const LogicalCTERef &source,
                                        const vector<ColumnBinding> &source_keys, vector<ColumnBinding> target_keys,
                                        const vector<ExpressionType> &comparisons) {
	if (!ContainsCTEJoinKeys(op, target_keys) || !CanPassCTEJoinFilter(op)) {
		return;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_CTE_REF:
		AddFilter(source, op.Cast<LogicalCTERef>(), source_keys, target_keys, comparisons);
		return;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		PushFilter(*op.children[1], source, source_keys, std::move(target_keys), comparisons);
		return;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		if (!MapCTEJoinProjection(op.Cast<LogicalProjection>(), target_keys)) {
			return;
		}
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		if (op.Cast<LogicalDistinct>().distinct_type != DistinctType::DISTINCT) {
			return;
		}
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (auto &key : target_keys) {
			if (key.table_index != aggregate.group_index) {
				return;
			}
			for (auto &grouping_set : aggregate.grouping_sets) {
				if (!grouping_set.count(key.column_index)) {
					return;
				}
			}
			auto &group = aggregate.GetGroupExpression(key.column_index);
			if (group.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return;
			}
			key = group.Cast<BoundColumnRefExpression>().Binding();
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNION: {
		auto bindings = op.GetColumnBindings();
		for (auto &child : op.children) {
			auto child_bindings = child->GetColumnBindings();
			auto child_keys = target_keys;
			for (auto &key : child_keys) {
				auto position = std::find(bindings.begin(), bindings.end(), key) - bindings.begin();
				key = child_bindings[position];
			}
			PushFilter(*child, source, source_keys, std::move(child_keys), comparisons);
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::INNER && join.join_type != JoinType::LEFT && join.join_type != JoinType::SEMI &&
		    join.join_type != JoinType::ANTI && join.join_type != JoinType::MARK) {
			return;
		}
		if (ContainsCTEJoinKeys(*op.children[0], target_keys)) {
			PushFilter(*op.children[0], source, source_keys, target_keys, comparisons);
		}
		if (join.join_type == JoinType::INNER && ContainsCTEJoinKeys(*op.children[1], target_keys)) {
			PushFilter(*op.children[1], source, source_keys, std::move(target_keys), comparisons);
		}
		return;
	}
	default:
		return;
	}
	PushFilter(*op.children[0], source, source_keys, std::move(target_keys), comparisons);
}

void CTEJoinFilterCollector::AddFilter(const LogicalCTERef &source, const LogicalCTERef &target,
                                       const vector<ColumnBinding> &source_keys,
                                       const vector<ColumnBinding> &target_keys,
                                       const vector<ExpressionType> &comparisons) {
	D_ASSERT(!comparisons.empty() && source_keys.size() == comparisons.size() &&
	         target_keys.size() == comparisons.size());
	auto target_entry = join_targets.find(target.cte_index);
	if (source.is_recurring || target.is_recurring || source.cte_index == target.cte_index ||
	    target_entry == join_targets.end() || target_entry->second != target.table_index) {
		return;
	}
	CTEJoinFilter filter;
	filter.source = source.cte_index;
	filter.target = target.cte_index;
	filter.target_scan = target.table_index;
	filter.comparisons = comparisons;
	for (idx_t i = 0; i < comparisons.size(); i++) {
		D_ASSERT(source_keys[i].table_index == source.table_index);
		D_ASSERT(target_keys[i].table_index == target.table_index);
		filter.source_columns.push_back(source_keys[i].column_index);
		filter.target_columns.push_back(target_keys[i].column_index);
	}
	for (auto &existing : join_filters) {
		if (existing.source == filter.source && existing.target == filter.target &&
		    existing.target_scan == filter.target_scan && existing.source_columns == filter.source_columns &&
		    existing.target_columns == filter.target_columns && existing.comparisons == filter.comparisons) {
			return;
		}
	}
	join_filters.push_back(std::move(filter));
}

} // namespace duckdb
