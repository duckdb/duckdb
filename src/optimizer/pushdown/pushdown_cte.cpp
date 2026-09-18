#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/cte_filter_pusher.hpp"
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

void FilterPushdown::CollectCTEJoinFilters(LogicalOperator &op, CTEFilterPusher &context) {
	for (auto &child : op.children) {
		CollectCTEJoinFilters(*child, context);
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
			PushCTEJoinFilter(*op.children[1 - side], *source, source_keys, side == 0 ? right_keys : left_keys,
			                  comparisons, context);
		}
	}
}

void FilterPushdown::PushCTEJoinFilter(LogicalOperator &op, const LogicalCTERef &source,
                                       const vector<ColumnBinding> &source_keys, vector<ColumnBinding> target_keys,
                                       const vector<ExpressionType> &comparisons, CTEFilterPusher &context) {
	if (!ContainsCTEJoinKeys(op, target_keys) || !CanPassCTEJoinFilter(op)) {
		return;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_CTE_REF:
		context.AddJoinFilter(source, op.Cast<LogicalCTERef>(), source_keys, target_keys, comparisons);
		return;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		PushCTEJoinFilter(*op.children[1], source, source_keys, std::move(target_keys), comparisons, context);
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
			PushCTEJoinFilter(*child, source, source_keys, std::move(child_keys), comparisons, context);
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
			PushCTEJoinFilter(*op.children[0], source, source_keys, target_keys, comparisons, context);
		}
		if (join.join_type == JoinType::INNER && ContainsCTEJoinKeys(*op.children[1], target_keys)) {
			PushCTEJoinFilter(*op.children[1], source, source_keys, std::move(target_keys), comparisons, context);
		}
		return;
	}
	default:
		return;
	}
	PushCTEJoinFilter(*op.children[0], source, source_keys, std::move(target_keys), comparisons, context);
}

} // namespace duckdb
